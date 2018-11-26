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

static const int map_size = 1024 * 1024 * 16;

polar_race::RetCode IndexStore::init(const std::string& dir, int party) {
    this->dir_ = dir;
    this->party_ = party;
    this->indexPath_ = dir + indexPrefix;
    if (!polar_race::FileExists(this->indexPath_) &&
        0 != mkdir(this->indexPath_.c_str(), 0755)) {
        fprintf(stderr, "[IndexStore] : mkdir failed %s\n", this->indexPath_.c_str());
        return polar_race::kIOError;
    }
    bool new_create = false;
    this->fileName_ = this->indexPath_ + std::to_string(party);
    int fd = open(this->fileName_.c_str(), O_RDWR, 0644);
    if (fd < 0 && errno == ENOENT) {
        // not exist, then create
        fd = open(this->fileName_.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd >= 0) {
            new_create = true;
            if (posix_fallocate(fd, 0, map_size) != 0) {
                fprintf(stderr, "[IndexStore] : posix_fallocate failed\n");
                close(fd);
                return polar_race::kIOError;
            }
        }
    }
    if (fd < 0) {
        fprintf(stderr, "[IndexStore] : file %s open failed\n", this->fileName_.c_str());
        return polar_race::kIOError;
    }
    this->fd_ = fd;

    void* ptr = mmap(NULL, map_size, PROT_READ | PROT_WRITE,
                     MAP_SHARED, this->fd_, 0);
    if (ptr == MAP_FAILED) {
        fprintf(stderr, "[IndexStore] : MAP_FAILED\n");
        close(fd);
        return polar_race::kIOError;
    }
    if (new_create) {
        fprintf(stderr, "[IndexStore] : create a new mmap \n");
        memset(ptr, 0, map_size);
    }
    items_ = reinterpret_cast<Item *>(ptr);
}

void IndexStore::add(const polar_race::PolarString& key, uint32_t info) {
    while (items_->info != 0) items_ ++;
    items_->info = info;
    memcpy(items_->key, key.data(), 8);
    items_++;
    this->size ++;
}

void IndexStore::get(const polar_race::PolarString& key, uint32_t* ans) {
    if (this->maps.empty()) initMaps();
    // radix_tree<std::string, long>::iterator ite = (*this->tree_).longest_match(key.ToString());
    std::map<std::string, long>::iterator ite = maps.find(std::string(key.data(), 8));
    if (ite != maps.end()) {
        (*ans) = ite->second;
    } else {
        (*ans) = 0;
        return;
    }
}

void IndexStore::initMaps() {
    // here we would init a radix tree from this structure.
   // this->tree_ = new radix_tree<std::string, long>();
    time_t t;
    time(&t);
    struct Item* temp = items_;
    while (temp->info > 0) {
        // (*this->tree_)[std::string(temp->key, 8)] = temp->info;
        maps[std::string(temp->key, 8)] = temp->info;
        this->size ++;
        temp++;
    }
    if (fd_ >= 0) {
        munmap(items_, map_size);
        close(fd_);
        fd_ = -1;
    }
    fprintf(stderr, "[IndexStore] : init radix_tree finished, total: %d data, taken %f s\n",
            this->size, difftime(time(NULL), t));
}

void IndexStore::finalize() {
    fprintf(stderr, "[IndexStore] : finalize index store\n");
    if (fd_ >= 0) {
        munmap(items_, map_size);
        close(fd_);
        fd_ = -1;
    }
    /*
    if (this->tree_ != NULL) {
        delete this->tree_;
        this->tree_ = NULL;
    }
    */
}

void IndexStore::rangeSearch(const polar_race::PolarString& lower, const polar_race::PolarString& upper,
                 polar_race::Visitor& visitor, polar_race::DataStore& store) {
    std::string value;
    for (std::map<std::string, long>::iterator ite = maps.begin(); ite != maps.end(); ite++) {
        long k = ite->second;
        uint16_t offset = polar_race::unwrapOffset(k);
        uint16_t fileNo = polar_race::unwrapFileNo(k);
        store.Read(fileNo, offset, &value);
        visitor.Visit(polar_race::PolarString(ite->first), polar_race::PolarString(value));
    }
}

IndexStore::~IndexStore() {
    fprintf(stderr, "[IndexStore] : finalize index store\n");
    if (fd_ >= 0) {
        munmap(items_, map_size);
        close(fd_);
        fd_ = -1;
    }
    /*
    if (this->tree_ != NULL) {
        delete this->tree_;
        this->tree_ = NULL;
    }
     */
}