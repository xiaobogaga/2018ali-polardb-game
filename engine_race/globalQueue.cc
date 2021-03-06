//
// Created by tomzhu on 18-12-1.
//

#include "globalQueue.h"

namespace polar_race {

    std::condition_variable notFullCV;
    std::mutex* mutexLocks;
    int realItemSizes[My_parties_];
    // just declare as volatile is not enough
    // needs to be atomic_int.
    // volatile int loaded = 0;
    // volatile int readedPart = -1;
    // change here.
    volatile int loaded;
    volatile int readedPart;
    std::condition_variable* loadCon;
    static const char My_kDataFilePrefix_[] = "DATA_";

    std::string FileName(const std::string &dir, uint32_t fileno) {
        return dir + "/" + My_kDataFilePrefix_ + std::to_string(fileno);
    }

    bool isPartCanLoad(int part, int readedPart) {
        return (part - readedPart) <= My_queueSize_;
    }

    bool isPartReady(int loaded, int part) {
        return loaded > part;
    }

    void initParams() {
        loaded = 0;
        readedPart = -1;
        // memset(realItemSizes, 0, sizeof(int) * My_parties_);
        for (int i = 0; i < My_parties_; i++) realItemSizes[i] = -1;
    }

    /**
 * loading data and return the size of total data.
 * @param data
 * @param indexStore
 * @return
 */
    int load(int part, struct QueueItem *data, DataStore *dataStore, IndexStore *indexStore) {
        time_t calc;
        time(&calc);
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
            if (fd < 0) printInfo(stderr, "[GlobalQueue] : ERROR. open file failed\n");
            lseek(fd, 0, SEEK_SET);
        }
        for (uint32_t i = 0; i < size; i++) {
            char *pos = data[i].data;
            value_len = My_valuesize_;
            fileRead ++;
            while (value_len > 0) {
                ssize_t r = read(fd, pos, value_len);
                if (r < 0) {
                    if (errno == EINTR) {
                        continue;  // Retry
                    }
                    close(fd);
                    printInfo(stderr, "[GlobalQueue] : ERROR. read file failed\n");
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
                        if (fd < 0) printInfo(stderr, "[GlobalQueue] : ERROR. open file failed\n");
                        lseek(fd, 0, SEEK_SET);
                    } else break;
                }
                pos += r;
                value_len -= r;
            }
        }
        if (fd >= 0) {
            // printInfo(stderr, "[GlobalQueue] : have read %d data for fileNo: %d under part %d\n",
            //        fileRead, j - 1, part);IndexStore
            close(fd);
        }
        printInfo(stderr, "[Loader] : load %d part finished with %d data and spend %f s\n",
                part, size, difftime(time(NULL), calc));
        return size;
    }

    /**
     * loader data.
     */
    void loaderMethod(DataStore *stores, IndexStore *indexStore, struct QueueItem** items) {
        for (int i = 0; i < My_parties_; i++) {
            std::unique_lock<std::mutex> lck(mutexLocks[i]);
            while (!isPartCanLoad(i, readedPart)) {
                if (My_exceedTime_) return;
                notFullCV.wait(lck);
                // wait or using computation.
                // let it out cpu.
                // std::this_thread::yield();
                // std::this_thread::sleep_for(std::chrono::microseconds(thread_spin_time));
            }
            // now we can load the data.
            // start loader data;
            // printInfo(stderr, "[GlobalQueue] : try loader one\n");
            struct QueueItem *data = items[i % My_queueSize_];
            // loading all data to queue.
            realItemSizes[i] = load(i, data, &stores[i], &indexStore[i]);

            // change here.
            // loaded += 1;
            loaded ++;
            loadCon[i].notify_all(); // is notify one here ? or notify all ?
        }
    }


    MessageQueue::MessageQueue(DataStore *stores_, IndexStore *indexStores_,
            std::mutex* mutexes, int threadSize) {
        printInfo(stderr, "[MessageQueue] : try to creating a message queue instance. with threadSize : %d\n"
                , threadSize);
        mutexLocks = mutexes;
        this->stores = stores_;
        this->indexStores = indexStores_;
        this->items = (struct QueueItem**) malloc(sizeof(struct QueueItem*) * My_queueSize_);
        for (int i = 0; i < My_queueSize_; i++) {
            this->items[i] = (struct QueueItem *) malloc(sizeof(struct QueueItem) * My_queueCapacity_);
            if (this->items[i] == NULL) printInfo(stderr, "[MessageQueue] : ERROR. malloc queueItem array failed\n");
        }
        this->loadCon_ = new std::condition_variable[My_parties_];
        loadCon = this->loadCon_;
        initParams(); // call initParams here.
        for (int i = 0; i < My_parties_; i++) {
            readCounter[i] = threadSize;
        }
        this->loader = new std::thread(loaderMethod, this->stores,
                                       this->indexStores, this->items);
    }

    // reading the data of i within this part, and partSize is the total size
    // of this part. for client, must iterator till partSize.
    char *MessageQueue::get(int readId, int part, int *i, int *partSize, char** k) {
        int idx = (*i);
        // only may block here.
        if (!My_exceedTime_) {
            if (idx == -1 || (idx == (*partSize))) { // read the first data of part.

                std::unique_lock<std::mutex> lck(mutexLocks[part]);
                while (!isPartReady(loaded, part)) {
                    if (My_exceedTime_) {
                        (*partSize) = 0;
                        return 0;
                    }

                    loadCon[part].wait(lck);
//                    // here we need to avoid using lock and condition variable.
                }

//here we try to use cas, we use realItemSizes[part] >= 0 to make sure part is loaded.
//                while (!isPartReady(loaded, part) && realItemSizes[part] < 0) {
//                    if (My_exceedTime_) {
//                        (*partSize) = 0;
//                        return 0;
//                    }
//                    // sleep current thread.
//                    // std::this_thread::yield();
//                    // std::this_thread::sleep_for(std::chrono::microseconds(thread_spin_time));
//                }


                // when data is coming.
                if (idx == -1) {
                    (*i) = 0;
                    (*partSize) = realItemSizes[part];
                }
                idx = (*i);
                if (idx == (*partSize)) {
                    // here must block, since lck lock might not used because idx != -1.
                    anotherLock.lock();
                    readCounter[part]--;
                    if (readCounter[readedPart + 1] == 0) {
                        // printInfo(stderr, "[GlobalQueue] : finalized %d part\n", readedPart + 1);
                        // indexStores[readedPart + 1].finalize();
                        readedPart ++;
                        notFullCV.notify_one();
                    }
                    anotherLock.unlock();
                    // here must block.
                    return NULL;

                }
            }
            uint32_t info = 0;
            (*i) = indexStores[part].getInfoAt(idx, k, &info);
            return items[part % My_queueSize_][(unwrapFileNo(info) - 1) *
                                                      My_kSingleFileSize_ / My_valuesize_
                                                      + unwrapOffset(info)].data;
        } else return NULL;
    }

}

