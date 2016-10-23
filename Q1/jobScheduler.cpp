#include <iostream>
#include <set>
#include <map>
#include <vector>
#include <string>
#include <istream>
#include <iterator>
#include <sstream>
#include <queue>
#include <unordered_map>
#include <climits>
using namespace std;

template <typename T>
void typecastStoN(string sId, T &id) {
	stringstream ss;
	ss << sId;
	ss >> id;
}

class job {
	private:
		unsigned long long jobId;
		unsigned long long timestamp;
		string systemName;
		string instructions;
		int importance;
		int duration;
		unsigned long long processedTimestamp;
	public:
		job(unsigned long long _timestamp) {
			timestamp = _timestamp;
			processedTimestamp = _timestamp;
			duration = INT_MAX;
		}
		job(vector<string> arguments) {
			typecastStoN(arguments[0], timestamp);
			typecastStoN(arguments[1], jobId);
			systemName = arguments[2];
			instructions = arguments[3];
			typecastStoN(arguments[4], importance);
			typecastStoN(arguments[5], duration);
			processedTimestamp = ULLONG_MAX;
		}
		job(unsigned long long _jobId, unsigned long long _timestamp, string _systemName, string _instructions, int _importance, int _duration) {
			jobId = _jobId;
			timestamp = _timestamp;
			systemName = _systemName;
			instructions = _instructions;
			importance = _importance;
			duration = _duration;
			processedTimestamp = ULLONG_MAX;
		}
		void setExpiration(unsigned long long timestamp) {
			processedTimestamp = timestamp;
		}
		unsigned long long getId() {
			return jobId;
		}
		unsigned long long getTimestamp() {
			return timestamp;
		}
		int getImportance() {
			return importance;
		}
		int getDuration() {
			return duration;
		}
		string getSystemName() {
			return systemName;
		}
		string getInstructions() {
			return instructions;
		}
		unsigned long long getExpiration() {
			return processedTimestamp;
		}
		void printJobDescription() {
			std::cout << "job " << this->timestamp << " " << this->jobId << " " << this->systemName << " "; 
			std::cout << this->instructions << " " << this->importance << " " << this->duration << "\n";
		}
};

class DereferenceCompareNode {
	public:
		bool operator() (job* lhs, job* rhs) const {
			unsigned long long leftprocessedTimestamp = lhs->getExpiration();
			unsigned long long rightProcessedTimestamp = rhs->getExpiration();
			if (leftprocessedTimestamp != rightProcessedTimestamp) {
				return (leftprocessedTimestamp < rightProcessedTimestamp);
			}
			unsigned long long leftTimestamp = lhs->getTimestamp();
			unsigned long long rightTimestamp = rhs->getTimestamp();
			if (leftTimestamp != rightTimestamp) {
				return (leftTimestamp < rightTimestamp);
			}
            return lhs->getDuration() < rhs->getDuration();
		}
};

class scheduler {
	private:
		static const int MAX_IMPORTANCE = 100;
		std::set < job*, DereferenceCompareNode> jobQueue[MAX_IMPORTANCE];
		std::set < job*, DereferenceCompareNode> processedJobs[MAX_IMPORTANCE];
		unordered_map<string, std::set <job*, DereferenceCompareNode> > jobDashboard[MAX_IMPORTANCE];
		std::multiset<unsigned long long> cpuJobTimestamps;
		queue < job* > unprocessedJobs;
		unsigned long long unUtilizedCpus;

		void addJob(job* newJob) {
			jobQueue[newJob->getImportance() - 1].insert(newJob);
			jobDashboard[newJob->getImportance() - 1][newJob->getSystemName()].insert(newJob);
			processedJobs[newJob->getImportance() - 1].insert(newJob);
		}

		void assign(unsigned long long timestamp, long long jobsCount) {
			for (int importance = MAX_IMPORTANCE - 1; importance >= 0; importance--) {
				while(!jobQueue[importance].empty() && jobsCount > 0) {
					if ((cpuJobTimestamps.empty())
					 || (*cpuJobTimestamps.begin() > timestamp)) {
						if (unUtilizedCpus == 0) {
							return;
						}
						// if there are any free unallocated cpus at this point insert a new to the memory.
						cpuJobTimestamps.insert(0);
						unUtilizedCpus -= 1;
					}
					// consider the highest importance job considered timestamp and duration ordered.
					job* nextJob = *jobQueue[importance].begin();

					// Print Job Description
					nextJob->printJobDescription();
					// Deleting the unprocessed job interval since it has been assigned to a cpu
					processedJobs[importance].erase(processedJobs[importance].find(nextJob));
					jobDashboard[importance][nextJob->getSystemName()].erase(
						jobDashboard[importance][nextJob->getSystemName()].find(nextJob));
					// update the job processing timestamp and insert into the history dashboard
					nextJob->setExpiration(timestamp);
					processedJobs[importance].insert(nextJob);
					jobDashboard[importance][nextJob->getSystemName()].insert(nextJob);

					// erase the job from the queue
					jobQueue[importance].erase(jobQueue[importance].begin());

					// Cpu Job Timestamps
					cpuJobTimestamps.erase(cpuJobTimestamps.begin());
					cpuJobTimestamps.insert(timestamp + nextJob->getDuration());
					jobsCount -= 1;
				}
			}
		}

		void addUnprocessedJobs(unsigned long long timestamp) {
			while(!unprocessedJobs.empty() && (unprocessedJobs.front()->getTimestamp() <= timestamp)) {
				addJob(unprocessedJobs.front());
				unprocessedJobs.pop();
			}
		}

		void assignJobs(unsigned long long timestamp, long long numberOfJobs) {
			addUnprocessedJobs(timestamp);
			assign(timestamp, numberOfJobs);
		}

		void query(unsigned long long timestamp, long long jobsCount) {
			addUnprocessedJobs(timestamp);
			job* jobQuery = new job(timestamp);
			for (int importance = MAX_IMPORTANCE - 1; importance >= 0; importance--) {
				auto jobIterator = processedJobs[importance].upper_bound(jobQuery);
				for (; jobIterator != processedJobs[importance].end();) {
					if (jobsCount <= 0) {
						return;
					}
					job* jobPointer = *jobIterator;
					if (jobPointer->getTimestamp() > timestamp) {
						break;
					}
					jobPointer->printJobDescription();
					jobsCount -= 1;
					jobIterator++;
				}
			}
			delete jobQuery;
		}

		void query(unsigned long long timestamp, string systemName) {
			addUnprocessedJobs(timestamp);
			job* jobQuery = new job(timestamp);
			for (int importance = MAX_IMPORTANCE - 1; importance >= 0; importance--) {
				auto jobIterator = jobDashboard[importance][systemName].upper_bound(jobQuery);
				for (; jobIterator != jobDashboard[importance][systemName].end();) {
					job* jobPointer = *jobIterator;
					if (jobPointer->getTimestamp() > timestamp) {
						break;
					}
					jobPointer->printJobDescription();
					jobIterator++;
				}
			}
			delete jobQuery;
		}

		bool isNum(char character) {
			return character >= '0' && character <= '9';
		}

		bool isAlphaNumeric(string arg) {
			for (auto argChar : arg) {
				if (!isNum(argChar)) {
					return true;
				}
			}
			return false;
		}
	public:
		scheduler(unsigned long long _cpus) {
			cpuJobTimestamps.clear();
			unUtilizedCpus = _cpus;
			for (int importance = 0; importance < MAX_IMPORTANCE; importance++) {
				processedJobs[importance].clear();
				jobDashboard[importance].clear();
				jobQueue[importance].clear();
			}
		}

		void performNextInstruction(vector<string> &arguments) {
			string argType = arguments[0];
			if (argType == "job") {
				job* newJob = new job(vector<string>(arguments.begin() + 1, arguments.end()));
				unprocessedJobs.push(newJob);
				return;
			}

			unsigned long long timestamp = 0;
			typecastStoN(arguments[1], timestamp);
			if (argType == "query") { 
				if (isAlphaNumeric(arguments[2])) {
					query(timestamp, arguments[2]);
				} else {
					long long numberOfJobs = 0;
					typecastStoN(arguments[2], numberOfJobs);
					query(timestamp, numberOfJobs);
				}
				return;
			}
			long long numberOfJobs = 0;
			typecastStoN(arguments[2], numberOfJobs);
			assignJobs(timestamp, numberOfJobs);
		}
};

void startScheduler() {
	unsigned long long numberOfCpus = 0;
	string instruction;
	std::cin >> instruction >> numberOfCpus;
	cin.clear();
	cin.ignore();
	scheduler priorityScheduler(numberOfCpus);
	std::vector<string> instructionList;
	while (getline(std::cin, instruction)) {
		if (instruction.length() == 0) {
			continue;
		}
		std::istringstream is(instruction);
		instructionList.clear();
   		std::copy( std::istream_iterator<string>(is), std::istream_iterator<string>(), std::back_inserter(instructionList));
		priorityScheduler.performNextInstruction(instructionList);
	}
}

int main(int argc, char **argv) {
	startScheduler();
	return 0;
}