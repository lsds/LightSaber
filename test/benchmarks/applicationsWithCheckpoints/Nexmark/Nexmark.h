#pragma once

#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <random>
#include <unordered_set>

#include "../BenchmarkQuery.h"
#include "utils/QueryApplication.h"
#include "utils/TupleSchema.h"
#include "utils/Utils.h"

class Nexmark : public BenchmarkQuery {
 private:
  struct InputSchema {
    long timestamp;
    long id;
    long itemName;
    long description;
    long initialBid;
    long reserve;
    long expires;
    long seller;
    long category;
    long padding_0;
    long padding_1;
    long padding_2;
    long padding_3;
    long padding_4;
    long padding_5;
    long padding_6;

    static void parse(InputSchema &tuple, std::string &line) {
      std::istringstream iss(line);
      std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                     std::istream_iterator<std::string>{}};
      tuple.timestamp = std::stol(words[0]);
      tuple.id = std::stol(words[1]);
      tuple.itemName = std::stol(words[2]);
      tuple.description = std::stol(words[3]);
      tuple.initialBid = std::stol(words[4]);
      tuple.reserve = std::stol(words[5]);
      tuple.expires = std::stol(words[6]);
      tuple.seller = std::stol(words[7]);
      tuple.category = std::stol(words[8]);
    }
  };

  /**
   * We start the ids at specific values to help ensure the queries find a match
   * even on small synthesized dataset sizes.
   */
  const long FIRST_AUCTION_ID = 1000L;
  const long FIRST_PERSON_ID = 1000L;
  const long FIRST_CATEGORY_ID = 10L;
  /** Proportions of people/auctions/bids to synthesize. */
  const int PERSON_PROPORTION = 1;

  const int AUCTION_PROPORTION = 3;
  const int BID_PROPORTION = 46;
  const int PROPORTION_DENOMINATOR =
      PERSON_PROPORTION + AUCTION_PROPORTION + BID_PROPORTION;

  /**
   * Keep the number of categories small so the example queries will find
   * results even with a small batch of events.
   */
  const int NUM_CATEGORIES = 5;
  /** Number of yet-to-be-created people and auction ids allowed. */
  const int AUCTION_ID_LEAD = 10;
  /**
   * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are
   * 1 over these values.
   */
  const int HOT_SELLER_RATIO = 100;

  /*
   * Extra parameters
   * */
  long outOfOrderGroupSize = 1;
  long firstEventNumber = 0;
  long firstEventId = 0;
  /** Number of yet-to-be-created people and auction ids allowed. */
  const int PERSON_ID_LEAD = 10;
  /** Average idealized size of a 'new person' event, in bytes. */
  int avgPersonByteSize = 200;
  /** Average idealized size of a 'new auction' event, in bytes. */
  int avgAuctionByteSize = 500;
  /** Average idealized size of a 'bid' event, in bytes. */
  int avgBidByteSize = 100;
  /** Ratio of bids to 'hot' auctions compared to all other auctions. */
  int hotAuctionRatio = 2;
  /** Ratio of auctions for 'hot' sellers compared to all other people. */
  int hotSellersRatio = 4;
  /** Ratio of bids for 'hot' bidders compared to all other people. */
  int hotBiddersRatio = 4;
  /** Window size, in seconds, for queries 3, 5, 7 and 8. */
  long windowSizeSec = 10;
  /** Sliding window period, in seconds, for query 5. */
  long windowPeriodSec = 5;
  /** Number of seconds to hold back events according to their reported
   * timestamp. */
  long watermarkHoldbackSec = 0;
  /** Average number of auction which should be inflight at any time, per
   * generator. */
  int numInFlightAuctions = 100;
  /** Maximum number of people to consider as active for placing auctions or
   * bids. */
  int numActivePeople = 1000;
  /** Initial overall event rate. */
  int firstEventRate = 10000;
  /** Next overall event rate. */
  int nextEventRate = 10000;
  /** Events per second **/
  const int eventsPerSec = 1000;

  long lastBase0AuctionId(long eventId) {
    long epoch = eventId / PROPORTION_DENOMINATOR;
    long offset = eventId % PROPORTION_DENOMINATOR;
    if (offset < PERSON_PROPORTION) {
      // About to generate a person.
      // Go back to the last auction in the last epoch.
      epoch--;
      offset = AUCTION_PROPORTION - 1;
    } else if (offset >= PERSON_PROPORTION + AUCTION_PROPORTION) {
      // About to generate a bid.
      // Go back to the last auction generated in this epoch.
      offset = AUCTION_PROPORTION - 1;
    } else {
      // About to generate an auction.
      offset -= PERSON_PROPORTION;
    }
    return epoch * AUCTION_PROPORTION + offset;
  }

  long lastBase0PersonId(long eventId) {
    long epoch = eventId / PROPORTION_DENOMINATOR;
    long offset = eventId % PROPORTION_DENOMINATOR;
    if (offset >= PERSON_PROPORTION) {
      // About to generate an auction or bid.
      // Go back to the last person generated in this epoch.
      offset = PERSON_PROPORTION - 1;
    }
    // About to generate a person.
    return epoch * PERSON_PROPORTION + offset;
  }

  long nextBase0PersonId(long eventId) {
    // Choose a random person from any of the 'active' people, plus a few
    // 'leads'. By limiting to 'active' we ensure the density of bids or
    // auctions per person does not decrease over time for long running jobs. By
    // choosing a person id ahead of the last valid person id we will make
    // newPerson and newAuction events appear to have been swapped in time.
    // todo: fix this
    std::random_device rd;
    std::mt19937_64 eng(rd());

    long numPeople = lastBase0PersonId(eventId) + 1;
    long activePeople = std::min(numPeople, (long)numActivePeople);

    std::uniform_int_distribution<long> distr(0, activePeople + PERSON_ID_LEAD);
    long n = distr(eng);
    return numPeople - activePeople + n;
  }

  long nextEventNumber(long numEvents) { return firstEventNumber + numEvents; }

  long nextAdjustedEventNumber(long numEvents) {
    long n = outOfOrderGroupSize;
    long eventNumber = nextEventNumber(numEvents);
    long base = (eventNumber / n) * n;
    long offset = (eventNumber * 953) % n;
    return base + offset;
  }

  long getNextEventId(long eventsCountSoFar) {
    return firstEventId + nextAdjustedEventNumber(eventsCountSoFar);
  }

  long getNextAuctionId(long eventsCountSoFar) {
    return FIRST_AUCTION_ID + nextAdjustedEventNumber(eventsCountSoFar);
  }

 public:
  TupleSchema *m_schema = nullptr;
  QueryApplication *m_application = nullptr;
  std::vector<char> *m_data = nullptr;
  bool m_debug = false;

  QueryApplication *getApplication() override { return m_application; }

  virtual void createApplication() = 0;

  void loadInMemoryData() {
    std::random_device rd;
    std::mt19937_64 eng(rd());
    std::uniform_int_distribution<long> distr(0, 1000000);

    std::random_device rd_;
    std::mt19937_64 eng_(rd_());
    std::uniform_real_distribution<> dbl(0.0, 1.0);

    std::random_device _rd;
    std::mt19937_64 _eng(_rd());
    std::uniform_int_distribution<int> _distr(HOT_SELLER_RATIO);

    std::random_device _rd_;
    std::mt19937_64 _eng_(_rd_());
    std::uniform_int_distribution<int> _distr_(0, NUM_CATEGORIES);

    size_t len = SystemConf::getInstance().BUNDLE_SIZE;
    m_data = new std::vector<char>(len);
    auto buf = (InputSchema *)m_data->data();

   std::unordered_set<long> set;

    std::string line;
    unsigned long idx = 0;
    long timestamp = 0;
    while (idx < len / sizeof(InputSchema)) {
      auto eventsCountSoFar = idx;
      auto newEventId = getNextEventId(eventsCountSoFar);

      if ((int)idx % eventsPerSec == 0) {
        timestamp++;
      }
      auto id = lastBase0AuctionId(newEventId) + FIRST_AUCTION_ID;
      set.insert(id);
      auto initialBid = std::round(std::pow(10.0, dbl(eng_) * 6.0) * 100.0);
      auto itemName = distr(eng);
      auto description = distr(eng);
      auto reserve =
          initialBid + std::round(std::pow(10.0, dbl(eng_) * 6.0) * 100.0);
      long seller;
      // Here P(auction will be for a hot seller) = 1 - 1/hotSellersRatio.
      if (_distr(_eng) > 0) {
        // Choose the first person in the batch of last HOT_SELLER_RATIO people.
        seller = (lastBase0PersonId(newEventId) / HOT_SELLER_RATIO) *
                 HOT_SELLER_RATIO;
      } else {
        seller = nextBase0PersonId(newEventId);
      }
      seller += FIRST_PERSON_ID;
      auto category = FIRST_CATEGORY_ID + _distr_(_eng_);
      auto expires = timestamp + distr(eng);

      line = std::to_string(timestamp) + " " + std::to_string(id) + " " +
             std::to_string(itemName) + " " + std::to_string(description) +
             " " + std::to_string(initialBid) + " " + std::to_string(reserve) +
             " " + std::to_string(expires) + " " + std::to_string(seller) +
             " " + std::to_string(category);
      InputSchema::parse(buf[idx], line);
      if (m_startTimestamp == 0) {
        m_startTimestamp = buf[0].timestamp;
      }
      m_endTimestamp = buf[idx].timestamp;
      idx++;
    }

    std::cout << "Distinct keys " << set.size() << std::endl;

    if (m_debug) {
      std::cout << "timestamp id itemName description initialBid reserve "
                   "expires seller category"
                << std::endl;
      for (unsigned long i = 0; i < m_data->size() / sizeof(InputSchema); ++i) {
        printf("[DBG] %09d: %7ld %13ld %8ld %13ld %3ld %6ld %2ld %6ld %6ld \n",
               i, buf[i].timestamp, (long)buf[i].id, (long)buf[i].itemName,
               (long)buf[i].description, buf[i].initialBid, buf[i].reserve,
               (long)buf[i].expires, (long)buf[i].seller,
               (long)buf[i].category);
      }
    }
  };

  std::vector<char> *getInMemoryData() override { return m_data; }

  TupleSchema *getSchema() override {
    if (m_schema == nullptr) createSchema();
    return m_schema;
  }

  std::vector<char> *getStaticData() override {
    throw std::runtime_error("error: this benchmark does not have static data");
  }

  void createSchema() {
    m_schema = new TupleSchema(16, "Nexmark"); // 9, "Nexmark");
    auto longAttr = AttributeType(BasicType::Long);

    m_schema->setAttributeType(0, longAttr); /*   timestamp:  long  */
    m_schema->setAttributeType(1, longAttr); /*          id:  long */
    m_schema->setAttributeType(2, longAttr); /*    itemName:  long */
    m_schema->setAttributeType(3, longAttr); /* description:  long */
    m_schema->setAttributeType(4, longAttr); /*  initialBid:  long  */
    m_schema->setAttributeType(5, longAttr); /*     reserve:  long  */
    m_schema->setAttributeType(6, longAttr); /*     expires:  long  */
    m_schema->setAttributeType(7, longAttr); /*      seller:  long */
    m_schema->setAttributeType(8, longAttr); /*    category:  long */
    m_schema->setAttributeType(9, longAttr); /*     padding:  long */
    m_schema->setAttributeType(10, longAttr); /*    padding:  long */
    m_schema->setAttributeType(11, longAttr); /*    padding:  long */
    m_schema->setAttributeType(12, longAttr); /*    padding:  long */
    m_schema->setAttributeType(13, longAttr); /*    padding:  long */
    m_schema->setAttributeType(14, longAttr); /*    padding:  long */
    m_schema->setAttributeType(15, longAttr); /*    padding:  long */
  }
};
