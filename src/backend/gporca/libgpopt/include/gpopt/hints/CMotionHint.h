//
// Created by Sanath Vobilisetty on 3/18/24.
//

#ifndef GPOS_CMotionHint_H
#define GPOS_CMotionHint_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CEnumSet.h"
#include "gpos/common/CEnumSetIter.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/hints/IHint.h"
#include "gpopt/operators/COperator.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"


namespace gpopt
{
class CMotionHint : public IHint, public DbgPrintMixin<CMotionHint>
{
public:
	enum MotionType
	{
		Broadcast,
		Redistribute,

		Sentinel
	};

private:
	CMemoryPool *m_mp;

	StringPtrArray *m_aliases{nullptr};

	MotionType m_motion_type;

public:
	CMotionHint(CMemoryPool *mp, StringPtrArray *aliases, MotionType type)
		: m_mp(mp), m_aliases(aliases), m_motion_type(type)
	{
		aliases->Sort(CWStringBase::Compare);
	}

	~CMotionHint()
	{
		CRefCount::SafeRelease(m_aliases);
	}

	const StringPtrArray *
	GetAliasNames() const
	{
		return m_aliases;
	}

	MotionType
	GetType() const
	{
		return m_motion_type;
	}

	IOstream &OsPrint(IOstream &os) const;

	void Serialize(CXMLSerializer *xml_serializer) const;
};

using MotionHintList = CDynamicPtrArray<CMotionHint, CleanupRelease>;

}  // namespace gpopt

#endif	//GPOS_CMotionHint_H

// EOF
