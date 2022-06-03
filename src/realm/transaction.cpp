/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/transaction.hpp>

namespace realm {

void Transaction::check_consistency()
{
    // For the time being, we only check if asymmetric table are empty
    std::vector<TableKey> needs_fix;
    auto table_keys = get_table_keys();
    for (auto tk : table_keys) {
        auto table = get_table(tk);
        if (table->is_asymmetric() && table->size() > 0) {
            needs_fix.push_back(tk);
        }
    }
    if (!needs_fix.empty()) {
        promote_to_write();
        for (auto tk : needs_fix) {
            get_table(tk)->clear();
        }
        commit();
    }
}

void Transaction::upgrade_file_format(int target_file_format_version)
{
    REALM_ASSERT(is_attached());
    if (fake_target_file_format && *fake_target_file_format == target_file_format_version) {
        // Testing, mockup scenario, not a real upgrade. Just pretend we're done!
        return;
    }

    // Be sure to revisit the following upgrade logic when a new file format
    // version is introduced. The following assert attempt to help you not
    // forget it.
    REALM_ASSERT_EX(target_file_format_version == 22, target_file_format_version);

    // DB::do_open() must ensure that only supported version are allowed.
    // It does that by asking backup if the current file format version is
    // included in the accepted versions, so be sure to align the list of
    // versions with the logic below

    int current_file_format_version = get_file_format_version();
    REALM_ASSERT(current_file_format_version < target_file_format_version);

    // Upgrade from version prior to 7 (new history schema version in top array)
    if (current_file_format_version <= 6 && target_file_format_version >= 7) {
        // If top array size is 9, then add the missing 10th element containing
        // the history schema version.
        std::size_t top_size = m_top.size();
        REALM_ASSERT(top_size <= 9);
        if (top_size == 9) {
            int initial_history_schema_version = 0;
            m_top.add(initial_history_schema_version); // Throws
        }
        set_file_format_version(7);
        commit_and_continue_writing();
    }

    // Upgrade from version prior to 10 (Cluster based db)
    if (current_file_format_version <= 9 && target_file_format_version >= 10) {
        DisableReplication disable_replication(*this);

        std::vector<TableRef> table_accessors;
        TableRef pk_table;
        TableRef progress_info;
        ColKey col_objects;
        ColKey col_links;
        std::map<TableRef, ColKey> pk_cols;

        // Use table lookup by name. The table keys are not generated yet
        for (size_t t = 0; t < m_table_names.size(); t++) {
            StringData name = m_table_names.get(t);
            // In file format version 9 files, all names represent existing tables.
            auto table = get_table(name);
            if (name == "pk") {
                pk_table = table;
            }
            else if (name == "!UPDATE_PROGRESS") {
                progress_info = table;
            }
            else {
                table_accessors.push_back(table);
            }
        }

        if (!progress_info) {
            // This is the first time. Prepare for moving objects in one go.
            progress_info = this->add_table_with_primary_key("!UPDATE_PROGRESS", type_String, "table_name");
            col_objects = progress_info->add_column(type_Bool, "objects_migrated");
            col_links = progress_info->add_column(type_Bool, "links_migrated");


            for (auto k : table_accessors) {
                k->migrate_column_info();
            }

            if (pk_table) {
                pk_table->migrate_column_info();
                pk_table->migrate_indexes(ColKey());
                pk_table->create_columns();
                pk_table->migrate_objects();
                pk_cols = get_primary_key_columns_from_pk_table(pk_table);
            }

            for (auto k : table_accessors) {
                k->migrate_indexes(pk_cols[k]);
            }
            for (auto k : table_accessors) {
                k->migrate_subspec();
            }
            for (auto k : table_accessors) {
                k->create_columns();
            }
            commit_and_continue_writing();
        }
        else {
            if (pk_table) {
                pk_cols = get_primary_key_columns_from_pk_table(pk_table);
            }
            col_objects = progress_info->get_column_key("objects_migrated");
            col_links = progress_info->get_column_key("links_migrated");
        }

        bool updates = false;
        for (auto k : table_accessors) {
            if (k->verify_column_keys()) {
                updates = true;
            }
        }
        if (updates) {
            commit_and_continue_writing();
        }

        // Migrate objects
        for (auto k : table_accessors) {
            auto progress_status = progress_info->create_object_with_primary_key(k->get_name());
            if (!progress_status.get<bool>(col_objects)) {
                bool no_links = k->migrate_objects();
                progress_status.set(col_objects, true);
                progress_status.set(col_links, no_links);
                commit_and_continue_writing();
            }
        }
        for (auto k : table_accessors) {
            auto progress_status = progress_info->create_object_with_primary_key(k->get_name());
            if (!progress_status.get<bool>(col_links)) {
                k->migrate_links();
                progress_status.set(col_links, true);
                commit_and_continue_writing();
            }
        }

        // Final cleanup
        for (auto k : table_accessors) {
            k->finalize_migration(pk_cols[k]);
        }

        if (pk_table) {
            remove_table("pk");
        }
        remove_table(progress_info->get_key());
    }

    // Ensure we have search index on all primary key columns. This is idempotent so no
    // need to check on current_file_format_version
    auto table_keys = get_table_keys();
    for (auto k : table_keys) {
        auto t = get_table(k);
        if (auto col = t->get_primary_key_column()) {
            t->do_add_search_index(col);
        }
    }

    // NOTE: Additional future upgrade steps go here.
}

Transaction::Transaction(DBRef _db, SlabAlloc* alloc, DB::ReadLockInfo& rli, DB::TransactStage stage)
    : Group(alloc)
    , db(_db)
    , m_read_lock(rli)
{
    bool writable = stage == DB::transact_Writing;
    m_transact_stage = DB::transact_Ready;
    set_metrics(db->m_metrics);
    set_transact_stage(stage);
    m_alloc.note_reader_start(this);
    attach_shared(m_read_lock.m_top_ref, m_read_lock.m_file_size, writable);
}

void Transaction::close()
{
    if (m_transact_stage == DB::transact_Writing) {
        rollback();
    }
    if (m_transact_stage == DB::transact_Reading || m_transact_stage == DB::transact_Frozen) {
        do_end_read();
    }
}

namespace {

template <class F>
void trv(ref_type ref, Allocator& alloc, std::vector<unsigned>& path, F&& func)
{
    MemRef mem(ref, alloc);
    auto hdr = mem.get_addr();
    auto sz = Node::get_size_from_header(hdr);
    auto w = Node::get_width_from_header(hdr);
    auto wtype = Node::get_wtype_from_header(hdr);
    auto byte_size = Node::calc_byte_size(wtype, sz, w);

    func(path, ref + byte_size);

    if (Node::get_hasrefs_from_header(hdr)) {
        path.push_back(0);
        auto data = Node::get_data_from_header(hdr);
        for (unsigned n = 0; n < sz; n++) {
            auto val = get_direct(data, w, n);
            if (val && !(val & 1)) {
                path.back() = n;
                trv(ref_type(val), alloc, path, func);
            }
        }
        path.pop_back();
    }
}

} // namespace

auto Transaction::get_outliers() const -> NodeTree
{
    std::vector<unsigned> path;
    NodeTree tree;
    if (size_t limit = m_top.get_as_ref_or_tagged(s_evacuation_point_ndx).get_as_int()) {
        trv(m_top.get_ref(), m_top.get_alloc(), path, [&](auto& path, size_t end) {
            if (end > limit) {
                auto current_tree = &tree;
                for (auto i : path) {
                    current_tree = &current_tree->children[i];
                }
            }
        });
    }
    return tree;
}

void Transaction::evacuate(const NodeTree& tree)
{
    if (size_t limit = m_top.get_as_ref_or_tagged(s_evacuation_point_ndx).get_as_int()) {
        if (auto it = tree.children.find(0); it != tree.children.end()) {
            it->second.cow(m_table_names, limit);
        }
        if (auto it = tree.children.find(1); it != tree.children.end()) {
            it->second.cow(m_tables, limit);
        }
        if (auto it = tree.children.find(8); it != tree.children.end()) {
            Array arr(m_top.get_alloc());
            arr.set_parent(&m_top, 8);
            arr.init_from_parent();
            it->second.cow(arr, limit);
        }
    }
}


void Transaction::end_read()
{
    if (m_transact_stage == DB::transact_Ready)
        return;
    if (m_transact_stage == DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);
    do_end_read();
}

void Transaction::do_end_read() noexcept
{
    prepare_for_close();
    detach();

    // We should always be ensuring that async commits finish before we get here,
    // but if the fsync() failed or we failed to update the top pointer then
    // there's not much we can do and we have to just accept that we're losing
    // those commits.
    if (m_oldest_version_not_persisted) {
        REALM_ASSERT(m_async_commit_has_failed);
        // We need to not release our read lock on m_oldest_version_not_persisted
        // as that's the version the top pointer is referencing and overwriting
        // that version will corrupt the Realm file.
        db->leak_read_lock(*m_oldest_version_not_persisted);
    }
    db->release_read_lock(m_read_lock);

    m_alloc.note_reader_end(this);
    set_transact_stage(DB::transact_Ready);
    // reset the std::shared_ptr to allow the DB object to release resources
    // as early as possible.
    db.reset();
}

TransactionRef Transaction::freeze()
{
    if (m_transact_stage != DB::transact_Reading)
        throw LogicError(LogicError::wrong_transact_state);
    auto version = VersionID(m_read_lock.m_version, m_read_lock.m_reader_idx);
    return db->start_frozen(version);
}

TransactionRef Transaction::duplicate()
{
    auto version = VersionID(m_read_lock.m_version, m_read_lock.m_reader_idx);
    if (m_transact_stage == DB::transact_Reading)
        return db->start_read(version);
    if (m_transact_stage == DB::transact_Frozen)
        return db->start_frozen(version);

    throw LogicError(LogicError::wrong_transact_state);
}

_impl::History* Transaction::get_history() const
{
    if (!m_history) {
        if (auto repl = db->get_replication()) {
            switch (m_transact_stage) {
                case DB::transact_Reading:
                case DB::transact_Frozen:
                    if (!m_history_read)
                        m_history_read = repl->_create_history_read();
                    m_history = m_history_read.get();
                    m_history->set_group(const_cast<Transaction*>(this), false);
                    break;
                case DB::transact_Writing:
                    m_history = repl->_get_history_write();
                    break;
                case DB::transact_Ready:
                    break;
            }
        }
    }
    return m_history;
}

void Transaction::rollback()
{
    // rollback may happen as a consequence of exception handling in cases where
    // the DB has detached. If so, just back out without trying to change state.
    // the DB object has already been closed and no further processing is possible.
    if (!is_attached())
        return;
    if (m_transact_stage == DB::transact_Ready)
        return; // Idempotency

    if (m_transact_stage != DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);
    db->reset_free_space_tracking();
    if (!holds_write_mutex())
        db->end_write_on_correct_thread();

    do_end_read();
}

size_t Transaction::get_commit_size() const
{
    size_t sz = 0;
    if (m_transact_stage == DB::transact_Writing) {
        sz = m_alloc.get_commit_size();
    }
    return sz;
}

DB::version_type Transaction::commit()
{
    if (!is_attached())
        throw LogicError(LogicError::wrong_transact_state);
    if (m_transact_stage != DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);

    REALM_ASSERT(is_attached());

    // before committing, allow any accessors at group level or below to sync
    flush_accessors_for_commit();

    DB::version_type new_version = db->do_commit(*this); // Throws

    // We need to set m_read_lock in order for wait_for_change to work.
    // To set it, we grab a readlock on the latest available snapshot
    // and release it again.
    VersionID version_id = VersionID(); // Latest available snapshot
    DB::ReadLockInfo lock_after_commit;
    db->grab_read_lock(lock_after_commit, version_id);
    db->release_read_lock(lock_after_commit);

    db->end_write_on_correct_thread();

    do_end_read();
    m_read_lock = lock_after_commit;

    return new_version;
}

void Transaction::commit_and_continue_writing()
{
    if (!is_attached())
        throw LogicError(LogicError::wrong_transact_state);
    if (m_transact_stage != DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);

    REALM_ASSERT(is_attached());

    // before committing, allow any accessors at group level or below to sync
    flush_accessors_for_commit();

    db->do_commit(*this); // Throws

    // We need to set m_read_lock in order for wait_for_change to work.
    // To set it, we grab a readlock on the latest available snapshot
    // and release it again.
    VersionID version_id = VersionID(); // Latest available snapshot
    DB::ReadLockInfo lock_after_commit;
    db->grab_read_lock(lock_after_commit, version_id);
    db->release_read_lock(m_read_lock);
    m_read_lock = lock_after_commit;
    if (Replication* repl = db->get_replication()) {
        bool history_updated = false;
        repl->initiate_transact(*this, lock_after_commit.m_version, history_updated); // Throws
    }

    bool writable = true;
    remap_and_update_refs(m_read_lock.m_top_ref, m_read_lock.m_file_size, writable); // Throws
}

void Transaction::initialize_replication()
{
    if (m_transact_stage == DB::transact_Writing) {
        if (Replication* repl = get_replication()) {
            auto current_version = m_read_lock.m_version;
            bool history_updated = false;
            repl->initiate_transact(*this, current_version, history_updated); // Throws
        }
    }
}

Transaction::~Transaction()
{
    // Note that this does not call close() - calling close() is done
    // implicitly by the deleter.
}

} // namespace realm
