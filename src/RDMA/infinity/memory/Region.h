/*
 * Memory - Region
 *
 * (c) 2018 Claude Barthels, ETH Zurich
 * Contact: claudeb@inf.ethz.ch
 *
 */

#ifndef MEMORY_REGION_H_
#define MEMORY_REGION_H_

#include <RDMA/infinity/core/Context.h>
#include <RDMA/infinity/memory/RegionType.h>
#include <infiniband/verbs.h>
#include <stdint.h>

namespace infinity {
namespace memory {

class RegionToken;

class Region {

public:

	virtual ~Region();

	RegionToken * createRegionToken();
	RegionToken * createRegionToken(uint64_t offset);
	RegionToken * createRegionToken(uint64_t offset, uint64_t size);

public:

	RegionType getMemoryRegionType();
	uint64_t getSizeInBytes();
	uint64_t getRemainingSizeInBytes(uint64_t offset);
	uint64_t getAddress();
	uint64_t getAddressWithOffset(uint64_t offset);
	uint32_t getLocalKey();
	uint32_t getRemoteKey();

protected:

	infinity::core::Context* context;
	RegionType memoryRegionType;
	ibv_mr *ibvMemoryRegion;

protected:

	void * data;
	uint64_t sizeInBytes;

};

} /* namespace memory */
} /* namespace infinity */

#endif /* MEMORY_REGION_H_ */
