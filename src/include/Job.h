
#ifndef WEBOFDATA_JOB_H
#define WEBOFDATA_JOB_H

#include <string>

namespace webofdata {

    using namespace std;

    class Job;

    class JobLogic {
        public:
            virtual void run() = 0;
            virtual void completed() = 0; 
            JobLogic(shared_ptr<Job> job);
    };

    class Job {
        private:
            std::shared_ptr<JobLogic> _jobLogic;
            long getLastOffset();
            void storeLastOffset();
        public:
            Job(shared_ptr<JobLogic> jobLogic) : _jobLogic(jobLogic) {}
            void start();
    };
}

#endif

