//
// Created by Sanath Vobilisetty on 3/19/24.
//

#include "gpopt/hints/CMotionHint.h"

#include "naucrates/dxl/CDXLUtils.h"

using namespace gpopt;

FORCE_GENERATE_DBGSTR(CMotionHint);

IOstream &
CMotionHint::OsPrint(IOstream &os) const
{
	CWStringDynamic *aliases =
		CDXLUtils::SerializeToCommaSeparatedString(m_mp, GetAliasNames());

	os << "MotionHint: ";

	switch (m_motion_type)
	{
		case Broadcast:
		{
			os << " " << GPOS_WSZ_LIT("Broadcast");
			break;
		}
		case Redistribute:
		{
			os << " " << GPOS_WSZ_LIT("Redistribute");
			break;
		}
		default:
		{
		}
	}

	os << "(" << aliases->GetBuffer() << ")";
	GPOS_DELETE(aliases);
	return os;
}

void
CMotionHint::Serialize(CXMLSerializer *xml_serializer GPOS_UNUSED) const
{
}