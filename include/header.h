#pragma once
#include <cstdint>
#include <iostream>
#include <mdbx.h++>

struct mdbx_wrapper
{
    static uintptr_t open_database(const char* filepath, int maxdbs, const mdbx::env::geometry* geom)
    {
        try
        {
            mdbx::env_managed::create_parameters create_params;
            create_params.file_mode_bits = 0755;
            create_params.use_subdirectory = true;
            if (geom != nullptr)
            {
                create_params.geometry = *geom;
            }

            mdbx::env_managed::operate_parameters operate_params;
            operate_params.max_maps = 8;
            operate_params.mode = mdbx::env::mode::write_file_io;
            operate_params.durability = mdbx::env::durability::lazy_weak_tail;

            mdbx::env_managed* db = new mdbx::env_managed(filepath, create_params, operate_params);
            db->set_sync_threshold(1024 * 1024 * 4);
            db->set_sync_period(4.0);

            return reinterpret_cast<uintptr_t>(db);
        }
        catch (const std::exception& e)
        {
            std::cerr << e.what() << '\n';
            return reinterpret_cast<uintptr_t>(nullptr);
        }
    }

    static void copy_database(const uintptr_t& ptr, const char* destination)
    {
        mdbx::env_managed* env = reinterpret_cast<mdbx::env_managed*>(ptr);
        try
        {
            env->copy(destination, false, false);
        }
        catch (const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
    }

    static void close_database(const uintptr_t& ptr)
    {
        mdbx::env_managed* env = reinterpret_cast<mdbx::env_managed*>(ptr);
        try
        {
            env->close();
        }
        catch (const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
    }

    static uintptr_t start_write_trx(const uintptr_t& env_ptr)
    {
        mdbx::env_managed* env = reinterpret_cast<mdbx::env_managed*>(env_ptr);
        mdbx::txn_managed* trx = new mdbx::txn_managed(env->start_write());

        return reinterpret_cast<uintptr_t>(trx);
    }

    static uintptr_t start_read_trx(const uintptr_t& env_ptr)
    {
        mdbx::env_managed* env = reinterpret_cast<mdbx::env_managed*>(env_ptr);
        mdbx::txn_managed* trx = new mdbx::txn_managed(env->start_read());

        return reinterpret_cast<uintptr_t>(trx);
    }

    static void commit_trx(const uintptr_t& trx_ptr)
    {
        mdbx::txn_managed* trx = reinterpret_cast<mdbx::txn_managed*>(trx_ptr);
        trx->commit();
    }

    static void commit_trx(const uintptr_t& trx_ptr, MDBX_commit_latency* latency)
    {
        mdbx::txn_managed* trx = reinterpret_cast<mdbx::txn_managed*>(trx_ptr);
        trx->commit(latency);
    }

    static void abort_trx(const uintptr_t& trx_ptr)
    {
        mdbx::txn_managed* trx = reinterpret_cast<mdbx::txn_managed*>(trx_ptr);
        trx->abort();
    }

    static void close_trx(const uintptr_t& trx_ptr)
    {
        mdbx::txn_managed* trx = reinterpret_cast<mdbx::txn_managed*>(trx_ptr);
        delete trx;
    }

    static uint32_t open_map(const uintptr_t& trx_ptr, const char* dbname)
    {
        mdbx::txn_managed* trx = reinterpret_cast<mdbx::txn_managed*>(trx_ptr);
        return trx->open_map(dbname).dbi;
    }

    static uint32_t create_map(const uintptr_t& trx_ptr, const char* dbname)
    {
        mdbx::txn_managed* trx = reinterpret_cast<mdbx::txn_managed*>(trx_ptr);
        return trx->create_map(dbname).dbi;
    }

    static void close_map(const uintptr_t& env_ptr, const uint32_t dbi)
    {
        mdbx::map_handle map(dbi);
        mdbx::env_managed* env = reinterpret_cast<mdbx::env_managed*>(env_ptr);

        env->close_map(map);
    }

    static void put_to_map(const uintptr_t& trx_ptr, const uint32_t dbi, const char* key, size_t key_len,
                           const char* value, size_t value_len)
    {
        mdbx::map_handle map(dbi);
        mdbx::txn_managed* trx = reinterpret_cast<mdbx::txn_managed*>(trx_ptr);

        trx->put(map, mdbx::slice(key, key_len), mdbx::slice(value, value_len), mdbx::put_mode::upsert);
    }

    static uintptr_t open_iterator(const uintptr_t& trx_ptr, const char* dbname)
    {
        mdbx::txn_managed* trx = reinterpret_cast<mdbx::txn_managed*>(trx_ptr);
        mdbx::map_handle map = trx->create_map(dbname);
        mdbx::cursor_managed* cursor = new mdbx::cursor_managed(trx->open_cursor(map));

        return reinterpret_cast<uintptr_t>(cursor);
    }

    static void close_iterator(const uintptr_t& iter_ptr)
    {
        mdbx::cursor_managed* cursor = reinterpret_cast<mdbx::cursor_managed*>(iter_ptr);
        delete cursor;
    }

    static mdbx::cursor::move_result iterator_get_first(const uintptr_t& iter_ptr)
    {
        mdbx::cursor_managed* cursor = reinterpret_cast<mdbx::cursor_managed*>(iter_ptr);
        return cursor->to_first(false);
    }

    static mdbx::cursor::move_result iterator_get_next(const uintptr_t& iter_ptr)
    {
        mdbx::cursor_managed* cursor = reinterpret_cast<mdbx::cursor_managed*>(iter_ptr);
        return cursor->to_next(false);
    }

    static mdbx::slice get_value(const uintptr_t& trx_ptr, const uint32_t dbi, const char* key, size_t key_len)
    {
        mdbx::map_handle map(dbi);
        mdbx::txn_managed* trx = reinterpret_cast<mdbx::txn_managed*>(trx_ptr);

        return trx->get(map, mdbx::slice(key, key_len));
    }
};