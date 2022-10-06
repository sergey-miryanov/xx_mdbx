#include <cstdio>
#include <header.h>
#include <map>
#include <msgpack.h>
#include <vector>

void print_database(const char* filename)
{
    uintptr_t env = mdbx_wrapper::open_database("./temp.mdbx", 8, nullptr);

    uintptr_t trx = mdbx_wrapper::start_read_trx(env);

    uintptr_t attr_db = mdbx_wrapper::open_map(trx, "attrs");
    uintptr_t iter = mdbx_wrapper::open_iterator(trx, "tree");

    for (mdbx::cursor::move_result mr = mdbx_wrapper::iterator_get_first(iter); mr.done != false;
         mr = mdbx_wrapper::iterator_get_next(iter))
    {
        std::string key((const char*)mr.key.iov_base, mr.key.iov_len);
        // std::string val((const char*)mr.value.iov_base, mr.value.iov_len);

        printf("key=%s id=%lld\n", key.c_str(), *(reinterpret_cast<long long*>(mr.value.iov_base)));

        mdbx::slice value = mdbx_wrapper::get_value(trx, attr_db, (const char*)mr.value.iov_base, mr.value.iov_len);

        msgpack_unpacked result;
        msgpack_unpacked_init(&result);

        size_t off = 0;
        msgpack_unpack_return ret = msgpack_unpack_next(&result, (const char*)value.iov_base, value.iov_len, &off);

        if (ret != MSGPACK_UNPACK_SUCCESS)
        {
            printf("\tunpacker error: %d\n", (int)ret);
        }
        else
        {
            msgpack_object obj = result.data;
            msgpack_object_print(stdout, obj);
            printf("\n");
        }
        msgpack_unpacked_destroy(&result);
    }

    mdbx_wrapper::close_iterator(iter);
    mdbx_wrapper::close_map(env, attr_db);
    mdbx_wrapper::close_trx(trx);
    mdbx_wrapper::close_database(env);
}

void copy_database(const char* src, const char* dst)
{
    uintptr_t env = mdbx_wrapper::open_database(src, 8, nullptr);
    mdbx_wrapper::copy_database(env, dst);
    mdbx_wrapper::close_database(env);
}

typedef std::vector<std::tuple<int64_t, mdbx::slice>> flat_tree_t;

flat_tree_t load_flat_tree(const char* filename)
{
    flat_tree_t tree;

    uintptr_t env = mdbx_wrapper::open_database("./temp.mdbx", 8, nullptr);

    uintptr_t trx = mdbx_wrapper::start_read_trx(env);

    uintptr_t attr_db = mdbx_wrapper::open_map(trx, "attrs");
    uintptr_t iter = mdbx_wrapper::open_iterator(trx, "tree");

    for (mdbx::cursor::move_result mr = mdbx_wrapper::iterator_get_first(iter); mr.done != false;
         mr = mdbx_wrapper::iterator_get_next(iter))
    {
        std::string key((const char*)mr.key.iov_base, mr.key.iov_len);
        mdbx::slice value = mdbx_wrapper::get_value(trx, attr_db, (const char*)mr.value.iov_base, mr.value.iov_len);

        char* new_value_buffer = new char[value.iov_len];
        memcpy(new_value_buffer, value.iov_base, value.iov_len);

        mdbx::slice tree_value{new_value_buffer, value.iov_len};
        tree.push_back(std::make_tuple(*reinterpret_cast<long long*>(mr.value.iov_base), tree_value));
    }

    mdbx_wrapper::close_iterator(iter);
    mdbx_wrapper::close_map(env, attr_db);
    mdbx_wrapper::close_trx(trx);
    mdbx_wrapper::close_database(env);

    return tree;
}

mdbx::slice drop_first_attr(const mdbx::slice& attr_data)
{
    const char* attr_mem = (const char*)attr_data.iov_base;
    size_t attr_mem_len = attr_data.iov_len;

    msgpack_unpacked result;
    msgpack_unpacked_init(&result);

    size_t off = 0;
    msgpack_unpack_return ret = msgpack_unpack_next(&result, attr_mem, attr_mem_len, &off);

    mdbx::slice new_attr_data(nullptr, (size_t)0);

    if (ret == MSGPACK_UNPACK_SUCCESS)
    {
        msgpack_object obj = result.data;
        if (obj.type != MSGPACK_OBJECT_ARRAY)
        {
            printf("\tdrop not array\n");
        }
        else
        {
            msgpack_sbuffer sbuf;
            msgpack_sbuffer_init(&sbuf);

            msgpack_packer pk;
            msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

            msgpack_pack_array(&pk, obj.via.array.size > 0 ? obj.via.array.size - 1 : 0);

            if (obj.via.array.size > 0)
            {
                msgpack_object* p = obj.via.array.ptr;
                msgpack_object* const pend = obj.via.array.ptr + obj.via.array.size;

                ++p;
                for (; p < pend; ++p)
                {
                    msgpack_pack_object(&pk, *p);
                }
            }

            char* new_attr_mem = new char[sbuf.size];
            memcpy(new_attr_mem, sbuf.data, sbuf.size);
            new_attr_data = mdbx::slice(new_attr_mem, sbuf.size);

            msgpack_sbuffer_destroy(&sbuf);
        }
    }
    else
    {
        printf("\tdrop unpacker error: %d\n", (int)ret);
    }

    msgpack_unpacked_destroy(&result);
    delete[] attr_mem;

    return new_attr_data;
}

void print_attrs(const mdbx::slice& attr_data)
{
    msgpack_unpacked result;
    msgpack_unpacked_init(&result);

    size_t off = 0;
    msgpack_unpack_return ret = msgpack_unpack_next(&result, (const char*)attr_data.iov_base, attr_data.iov_len, &off);

    if (ret != MSGPACK_UNPACK_SUCCESS)
    {
        printf("\tunpacker error: %d\n", (int)ret);
    }
    else
    {
        msgpack_object obj = result.data;
        msgpack_object_print(stdout, obj);
        printf("\n");
    }
    msgpack_unpacked_destroy(&result);
}

void print_latency(const MDBX_commit_latency& latency)
{
    printf("latency(ms): preparation=%.2f gc=%.2f write=%.2f sync=%.2f ending=%.2f whole=%.2f\n",
           latency.preparation * 1000.0 / 65536, latency.gc * 1000.0 / 65536, latency.write * 1000.0 / 65536,
           latency.sync * 1000.0 / 65536, latency.ending * 1000.0 / 65536, latency.whole * 1000.0 / 65536);
}

int main()
{
    // print_database("./temp.mdbx");
    std::remove("./temp2.mdbx");
    std::remove("./temp2.mdbx-lck");
    copy_database("./temp.mdbx", "./temp2.mdbx");

    flat_tree_t flat_tree = load_flat_tree("./temp2.mdbx");
    uintptr_t env = mdbx_wrapper::open_database("./temp2.mdbx", 8, nullptr);

    std::vector<MDBX_commit_latency> latencies;
    int count = 0;
    LARGE_INTEGER time1, time2;
    LARGE_INTEGER Frequency;

    QueryPerformanceCounter(&time1);
    QueryPerformanceFrequency(&Frequency);

    for (auto& node : flat_tree)
    {
        int64_t attr_idx = std::get<0>(node);
        mdbx::slice attr_data = std::get<1>(node);

        // print_attrs(attr_data);

        mdbx::slice new_attr_data = drop_first_attr(attr_data);
        for (; new_attr_data.iov_len > 1; new_attr_data = drop_first_attr(new_attr_data))
        {
            count += 1;

            uintptr_t trx = mdbx_wrapper::start_write_trx(env);
            uint32_t attrs_db = mdbx_wrapper::open_map(trx, "attrs");

            mdbx_wrapper::put_to_map(trx, attrs_db, reinterpret_cast<const char*>(&attr_idx), sizeof(attr_idx),
                                     (const char*)new_attr_data.iov_base, new_attr_data.iov_len);

            MDBX_commit_latency latency;
            mdbx_wrapper::commit_trx(trx, &latency);

            latencies.push_back(latency);
        }
    }

    QueryPerformanceCounter(&time2);
    double time_ms = (time2.QuadPart - time1.QuadPart) * 1000.0 / Frequency.QuadPart;

    printf("count=%d time=%.2f ms\n", count, time_ms);

    MDBX_commit_latency total{0, 0, 0, 0, 0, 0, 0};
    for (auto& latency : latencies)
    {
        total.preparation += latency.preparation;
        total.gc += latency.gc;
        total.write += latency.write;
        total.sync += latency.sync;
        total.ending += latency.ending;
        total.whole += latency.whole;
    }

    print_latency(total);

    mdbx_wrapper::close_database(env);

    return 0;
}