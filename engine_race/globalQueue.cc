//
// Created by tomzhu on 18-12-1.
//

#include "globalQueue.h"

namespace polar_race {

    bool isPartCanLoad(int part) {
        return (part - readedPart) <= queueSize;
    }

    bool isPartReady(int loaded, int part) {
        return loaded > part;
    }

    void initParams() {
        loaded = 0;
        readedPart = -1;
        for (int i = 0; i < queueSize; i++) realItemSizes[i] = 0;
    }

    /**
 * loading data and return the size of total data.
 * @param data
 * @param indexStore
 * @return
 */
    int load(int part, struct QueueItem *data, DataStore *dataStore, IndexStore *indexStore) {
        if (indexStore->getInfo() == NULL) indexStore->initMaps();
        uint32_t size = indexStore->getSize();
        int j = 1;
        uint32_t value_len = 0;
        std::string dataFilePath = dataStore->getDataFilePath();
        int fSize = dataStore->initFD();
        int fd = -1;
        int fileRead = 0;
        if (size > 0) {
            fd = open(FileName(dataFilePath, j++).c_str(), O_RDONLY, 0644);
            if (fd < 0) printInfo(stderr, "[GlobalQueue] : open file failed\n");
            lseek(fd, 0, SEEK_SET);
        }
        for (int i = 0; i < size; i++) {
            char *pos = data[i].data;
            value_len = valuesize;
            fileRead ++;
            while (value_len > 0) {
                ssize_t r = read(fd, pos, value_len);
                if (r < 0) {
                    if (errno == EINTR) {
                        continue;  // Retry
                    }
                    close(fd);
                //    printInfo(stderr, "[GlobalQueue] : read file failed\n");
                    return -1;
                }
                if (r == 0) {
                    // close and open new file.
                //    printInfo(stderr, "[GlobalQueue] : have read %d data for fileNo: %d under part %d\n",
                //            fileRead, j - 1, part);
                    close(fd);
                    fd = -1;
                    if (j <= fSize) {
                        fileRead = 0;
                        fd = open(FileName(dataFilePath, j++).c_str(), O_RDONLY, 0644);
                        if (fd < 0) printInfo(stderr, "[GlobalQueue] : open file failed\n");
                        lseek(fd, 0, SEEK_SET);
                    } else break;
                }
                pos += r;
                value_len -= r;
            }
        }
        if (fd >= 0) {
            // printInfo(stderr, "[GlobalQueue] : have read %d data for fileNo: %d under part %d\n",
            //        fileRead, j - 1, part);
            close(fd);
        }
        printInfo(stderr, "[Loader] : load %d part finished with %d data\n", part, size);
        return size;
    }

    /**
     * loader data.
     */
    void loaderMethod(MessageQueue* messageQueue, DataStore *stores, IndexStore *indexStore, struct QueueItem** items) {
        for (int i = 0; i < parties; i++) {
            std::unique_lock<std::mutex> lck(mutexLocks[i]);
            while (!isPartCanLoad(i)) {
                if (exceedTime) return;
                messageQueue->notFullCV.wait(lck);
            }
            // now we can load the data.
            // start loader data;
            // printInfo(stderr, "[GlobalQueue] : try loader one\n");
            struct QueueItem *data = items[i % queueSize];
            // loading all data to queue.
            realItemSizes[i % queueSize] = load(i, data, &stores[i], &indexStore[i]);
            loaded += 1;
            loadCon[i].notify_all(); // is notify one here ? or notify all ?
        }

    }


    MessageQueue::MessageQueue(DataStore *stores_, IndexStore *indexStores_, std::mutex* mutexes) {
        printInfo(stderr, "[MessageQueue] : try to creating a message queue instance\n");
        mutexLocks = mutexes;
        this->stores = stores_;
        this->indexStores = indexStores_;
        this->items = (struct QueueItem**) malloc(sizeof(struct QueueItem*) * queueSize);
        for (int i = 0; i < queueSize; i++) {
            this->items[i] = (struct QueueItem *) malloc(sizeof(struct QueueItem) * queueCapacity);
            if (this->items[i] == NULL) printInfo(stderr, "[MessageQueue] : malloc queueItem array failed\n");
        }
        this->loadCon_ = new std::condition_variable[parties];
        loadCon = this->loadCon_;
        initParams(); // call initParams here.

        this->loader = new std::thread(loaderMethod, this, this->stores,
                                       this->indexStores, this->items);
        for (int i = 0; i < parties; i++) {
            readCounter[i] = threadSize;
        }
    }

    // reading the data of i within this part, and partSize is the total size
    // of this part. for client, must iterator till partSize.
    char *MessageQueue::get(int part, int *i, int *partSize, long long *k) {
        int idx = (*i);
        // only may block here.
        if (!exceedTime) {
            if (idx == -1 || (idx == (*partSize))) { // read the first data of part.
                std::unique_lock<std::mutex> lck(mutexLocks[part]);
                while (!isPartReady(loaded, part)) {
                    if (exceedTime) {
                        (*partSize) = 0;
                        return 0;
                    }
                    loadCon[part].wait(lck);
                }
                // when data is coming.
                if (idx == -1) {
                    (*i) = 0;
                    (*partSize) = realItemSizes[part % queueSize];
                }
                idx = (*i);
                if (idx == (*partSize)) {
                    // here must block, since lck lock might not used because idx != -1.
                    readCounter[part]--;
                    anotherLock.lock();
                    if (readCounter[readedPart + 1] == 0) {
                        printInfo(stderr, "[GlobalQueue] : finalized %d part\n", readedPart + 1);
                        indexStores[readedPart + 1].finalize();
                        readedPart += 1;
                        // clear previous data.
                        notFullCV.notify_one();
                    }
                    anotherLock.unlock();
                    // here must block.
                    return NULL;
                }
            }
            uint32_t info = 0;
            (*i) = indexStores[part].getInfoAt(idx, k, &info);
            return items[part % queueSize][(unwrapFileNo(info) - 1) *
                                           kSingleFileSize / valuesize + unwrapOffset(info)].data;
        } else return NULL;
    }

}

