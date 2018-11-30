//
// Created by tomzhu on 18-11-24.
//

#include "indexstore.h"
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include "util.h"
#include <chrono>


namespace polar_race {

    int compare(const void *a, const void *b) {
        struct Info *infoA = (struct Info *) a;
        struct Info *infoB = (struct Info *) b;
        if (infoA->key == infoB->key) {
            // duplated key.
            uint16_t fileNoA = unwrapFileNo(infoA->info);
            uint16_t fileNoB = unwrapFileNo(infoB->info);
            if (fileNoA == fileNoB) {
                return unwrapOffset(infoA->info) <
                       unwrapOffset(infoB->info) ? -1 : 1;
            } else {
                return fileNoA > fileNoB ? 1 : -1;
            }

        }
        return infoA->key < infoB->key ? -1 : 1;
    }

    int bcompare(const void *a, const void *b) {
        long long infoA = (*(long long *) a);
        struct Info *infoB = (struct Info *) b;
        if (infoA == infoB->key) return 0;
        return infoA < infoB->key ? -1 : 1;
    }

    RetCode IndexStore::init(const std::string &dir, int party) {
        this->dir_ = dir;
        this->party_ = party;

        if (!FileExists(dir_)
            && 0 != mkdir(dir_.c_str(), 0755)) {
            fprintf(stderr, "[IndexStore-%d] : %s mkdir failed\n", party, dir_.c_str());
            return kIOError;
        }

        this->indexPath_ = dir + indexPrefix;
        if (!FileExists(this->indexPath_) &&
            0 != mkdir(this->indexPath_.c_str(), 0755)) {
            fprintf(stderr, "[IndexStore-%d] : mkdir failed %s\n", party, this->indexPath_.c_str());
            return kIOError;
        }
        bool new_create = false;
        this->fileName_ = this->indexPath_ + std::to_string(party);
        int fd = open(this->fileName_.c_str(), O_RDWR, 0644);
        size_t fileLength;
        newMapSize = map_size;
        if (fd < 0 && errno == ENOENT) {
            // not exist, then create
            fd = open(this->fileName_.c_str(), O_RDWR | O_CREAT, 0644);
            if (fd >= 0) {
                new_create = true;
                if (posix_fallocate(fd, 0, newMapSize) != 0) {
                    fprintf(stderr, "[IndexStore-%d] : posix_fallocate failed\n", party);
                    close(fd);
                    return kIOError;
                }
            }
        }

        if (fd < 0) {
            fprintf(stderr, "[IndexStore-%d] : file %s open failed\n", party, this->fileName_.c_str());
            return kIOError;
        } else {
            fileLength = GetFileLength(this->fileName_);
            if (fileLength > newMapSize) newMapSize = fileLength;
        }
        this->fd_ = fd;

        void *ptr = mmap(NULL, newMapSize, PROT_READ | PROT_WRITE,
                         MAP_SHARED, this->fd_, 0);
        if (ptr == MAP_FAILED) {
            fprintf(stderr, "[IndexStore-%d] : MAP_FAILED\n", party);
            close(fd);
            return kIOError;
        }
        if (new_create) {
            //   fprintf(stderr, "[IndexStore] : create a new mmap \n");
            memset(ptr, 0, newMapSize);
        }
        items_ = reinterpret_cast<Item *>(ptr);
        head_ = items_;
        this->start = newMapSize;
        this->sep = newMapSize / sizeof(struct Item);
        // initMaps();
        return RetCode::kSucc;
    }

    void IndexStore::reAllocate() {
        // needs reallocate.
        // fprintf(stderr, "[IndexStore] : reallocating\n", party_);
        if (munmap(head_, newMapSize) == -1) fprintf(stderr, "[IndexStore-%d] : unmap  failed\n", party_);
        if (posix_fallocate(this->fd_, start, map_size) != 0) {
            fprintf(stderr, "[IndexStore-%d] : posix_fallocate failed\n", party_);
            close(this->fd_);
            return;
        }
        void *ptr = mmap(NULL, map_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED, this->fd_, this->start);
        memset(ptr, 0, map_size);
        this->start += map_size;
        this->newMapSize = map_size;
        this->sep = newMapSize / sizeof(struct Item);
        if (ptr == MAP_FAILED) {
            fprintf(stderr, "[IndexStore -%d] : MAP_FAILED\n", party_);
            close(this->fd_);
            return;
        }
        items_ = reinterpret_cast<Item *>(ptr);
        head_ = items_;
    }

    void IndexStore::add(const PolarString &key, uint32_t info) {
        while ((items_ - head_ < this->sep) && items_->info != 0) items_++;
        if (items_ - head_ >= this->sep) reAllocate();
        items_->info = info;
        memcpy(items_->key, key.data(), 8);
        items_++;
        this->size++;
    }

    void IndexStore::get(long long key, uint32_t *ans) {
        if (this->infos == NULL) initMaps();

        if (this->bf != NULL && !this->bf->contains(key)) {
            (*ans) = 0;
            return;
        }

        struct Info *ret = (struct Info *) bsearch(&key, this->infos, this->size,
                                                   sizeof(struct Info), bcompare);

        // make sure it is the latest.
        while (ret != NULL && ret < (this->infos + this->size - 1) && (ret + 1)->key == key) {
            ret++;
        }

        if (ret == NULL) {
            (*ans) = 0;
            return;
        } else {
            (*ans) = ret->info;
        }

    }

    void IndexStore::initMaps2() {
        this->table = new MyHashTable();
        // fprintf(stderr, "[IndexStore-%d] : try to init map\n", party_);
        time_t t;
        time(&t);
        struct Item *temp = head_;
        this->size = 0;
        while (temp->info != 0) {
            long long k = strToLong(temp->key);
            this->table->add(k, temp->info);
            this->size++;
            temp++;
        }
        if (fd_ >= 0) {
            munmap(head_, newMapSize);
            close(fd_);
            fd_ = -1;
            head_ = NULL;
        }
        fprintf(stderr, "[IndexStore-%d] : init radix_tree finished, total: %d data, taken %f s\n",
                party_, this->size, difftime(time(NULL), t));
    }

    void IndexStore::get2(long long key, uint32_t *ans) {
        if (this->table == NULL) initMaps2();
        uint32_t ret = this->table->get(key);
        (*ans) = ret;
    }

    void IndexStore::initInfos() {
        this->infos = (struct Info *) malloc(sizeof(struct Info) * total);
        if (this->infos == NULL) {
            fprintf(stderr,
                    "[IndexStore-%d] : opps try to create info array to %d failed\n", party_, total);
        }
        this->bfparameters = new bloom_parameters();
        this->bfparameters->projected_element_count = bf_capacity;
        this->bfparameters->false_positive_probability = bf_p; // 1 in 10000
        this->bfparameters->random_seed = std::chrono::system_clock::now().time_since_epoch().count();
        if (!this->bfparameters) {
            fprintf(stderr, "[MyHashTable] : Invalid set of bloom filter parameters!\n");
            return;
        }
        this->bfparameters->compute_optimal_parameters();
        this->bf = new bloom_filter(*this->bfparameters);
    }

    void IndexStore::initMaps() {
        // here we would init a radix tree from this structure.
        // fprintf(stderr, "[IndexStore-%d] : try to init map\n", party_);
        uint32_t total1 = total;
        time_t t;
        time(&t);
        struct Item *temp = head_;
        this->size = 0;
        while (temp->info != 0) {
            if (this->infos == NULL) {
                initInfos();
            }
            if (this->size >= total1) {
                total1 *= 2;
                this->infos = (struct Info *) realloc(this->infos, sizeof(struct Info) * total1);
                if (this->infos == NULL) {
                    fprintf(stderr,
                            "[IndexStore-%d] : opps try to larger infos array to %d failed\n", party_, total1);
                }
            }
            long long kk = strToLong(temp->key);
            this->infos[this->size].key = kk;
            this->infos[this->size].info = temp->info;
            this->bf->insert(kk);
            this->size++;
            temp++;
        }
        if (fd_ >= 0) {
            munmap(head_, newMapSize);
            close(fd_);
            fd_ = -1;
            head_ = NULL;
            items_ = NULL;
        }
        if (this->infos == NULL) this->infos = (struct Info *) malloc(sizeof(struct Info));
        qsort(infos, this->size, sizeof(struct Info), compare);
        fprintf(stderr, "[IndexStore-%d] : init radix_tree finished, total: %d data, taken %f s\n",
                party_, this->size, difftime(time(NULL), t));
    }

    void IndexStore::finalize() {
        // fprintf(stderr, "[IndexStore] : finalize index store with size %d\n", this->size);
        if (fd_ >= 0) {
            // items_ = NULL;
            munmap(head_, newMapSize);
            items_ = NULL;
            head_ = NULL;
            close(fd_);
            fd_ = -1;
        }

        if (this->infos != NULL) {
            free(this->infos);
            this->infos = NULL;
        }

        if (this->table != NULL) {
            delete this->table;
            this->table = NULL;
        }

        if (this->bf != NULL) {
            delete this->bf;
            this->bf = NULL;
            delete this->bfparameters;
            this->bfparameters = NULL;
        }


    }

// init map for range search.
    void IndexStore::initMaps3() {
        // here we would init a radix tree from this structure.
        // fprintf(stderr, "[IndexStore-%d] : try to init map\n", party_);
        uint32_t total1 = total;
        time_t t;
        time(&t);
        struct Item *temp = head_;
        this->size = 0;
        while (temp->info != 0) {
            if (this->infos == NULL) {
                this->infos = (struct Info *) malloc(sizeof(struct Info) * total);
                if (this->infos == NULL) {
                    fprintf(stderr,
                            "[IndexStore-%d] : opps try to create info array to %d failed\n", party_, total);
                }
            }
            if (this->size >= total1) {
                total1 *= 2;
                this->infos = (struct Info *) realloc(this->infos, sizeof(struct Info) * total1);
                if (this->infos == NULL) {
                    fprintf(stderr,
                            "[IndexStore-%d] : opps try to larger infos array to %d failed\n", party_, total1);
                }
            }
            long long kk = strToLong(temp->key);
            this->infos[this->size].key = kk;
            this->infos[this->size].info = temp->info;
            this->size++;
            temp++;
        }
        if (fd_ >= 0) {
            munmap(head_, newMapSize);
            close(fd_);
            fd_ = -1;
            head_ = NULL;
        }
        if (this->infos == NULL) this->infos = (struct Info *) malloc(sizeof(struct Info));
        qsort(infos, this->size, sizeof(struct Info), compare);
        fprintf(stderr, "[IndexStore-%d] : init radix_tree finished, total: %d data, taken %f s\n",
                party_, this->size, difftime(time(NULL), t));

    }

    int IndexStore::rangeSearch(const PolarString &lower, const PolarString &upper,
                                Visitor **visitor, int vSize, DataStore *store) {
        time_t timer;
        time(&timer);
        std::string value;
        int ans = 0;
        if (this->infos == NULL) initMaps3();
        char buf[8];
        for (uint32_t i = 0; i < this->size; i++) {
            if (i + 1 < this->size && this->infos[i + 1].key == this->infos[i].key) continue;
            uint32_t k = this->infos[i].info;
            uint16_t offset = unwrapOffset(k);
            uint16_t fileNo = unwrapFileNo(k);
            store->Read(fileNo, offset, &value);
            longToStr(this->infos[i].key, buf);
            PolarString keyP(buf, 8);
            PolarString valueP(value);
            for (int j = 0; j < vSize; j++) visitor[j]->Visit(keyP, valueP);
            ans++;
        }
        fprintf(stderr, "[IndexStore-%d] : finished range search within %f s under %d data\n",
                party_, difftime(time(NULL), timer), ans);
        return ans;

    }


    IndexStore::~IndexStore() {
        fprintf(stderr, "[IndexStore-%d] : finalize index store. have saving data : %ld\n", party_, this->size);
        if (fd_ >= 0) {
            munmap(head_, newMapSize);
            close(fd_);
            fd_ = -1;
            items_ = NULL;
            head_ = NULL;
        }
        if (this->infos != NULL) {
            free(this->infos);
            this->infos = NULL;
        }

        if (this->table != NULL) {
            delete this->table;
            this->table = NULL;
        }

        if (this->bf != NULL) {
            delete this->bf;
            this->bf = NULL;
            delete this->bfparameters;
            this->bfparameters = NULL;
        }

    }

}