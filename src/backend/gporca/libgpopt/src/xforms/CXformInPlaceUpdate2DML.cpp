//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformUpdate2DML.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformInPlaceUpdate2DML.h"

#include "gpos/base.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CLogicalPartitionSelector.h"
#include "gpopt/operators/CLogicalSplit.h"
#include "gpopt/operators/CLogicalUpdate.h"
#include "gpopt/operators/CLogicalInPlaceUpdate.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CScalarDMLAction.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDTypeInt4.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformUpdate2DML::CXformUpdate2DML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInPlaceUpdate2DML::CXformInPlaceUpdate2DML(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalInPlaceUpdate(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUpdate2DML::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformInPlaceUpdate2DML::Exfp(CExpressionHandle &	// exprhdl
) const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUpdate2DML::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformInPlaceUpdate2DML::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
							CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalInPlaceUpdate *popUpdate = CLogicalInPlaceUpdate::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components for alternative

	CTableDescriptor *ptabdesc = popUpdate->Ptabdesc();
	CColRefArray *pdrgpcrInsert = popUpdate->PdrgpcrInsert();
	CColRef *pcrCtid = popUpdate->PcrCtid();
	CColRef *pcrSegmentId = popUpdate->PcrSegmentId();

	COptCtxt *poctxt = COptCtxt::PoctxtFromTLS();
	CMDAccessor *md_accessor = poctxt->Pmda();
	CColumnFactory *col_factory = poctxt->Pcf();

	const IMDType *pmdtype = md_accessor->PtMDType<IMDTypeInt4>();
	CColRef *pcrAction = col_factory->PcrCreate(pmdtype, default_type_modifier);

	// child of update operator
	CExpression *pexprChild = (*pexpr)[0];
	pexprChild->AddRef();
	ptabdesc->AddRef();
	pdrgpcrInsert->AddRef();


	CExpression *pexprDML = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CLogicalDML(
			mp, CLogicalDML::EdmlInPlaceUpdate, ptabdesc, pdrgpcrInsert, GPOS_NEW(mp) CBitSet(mp),
			pcrAction, nullptr, pcrCtid, pcrSegmentId, nullptr),
		pexprChild);

	// TODO:  - Oct 30, 2012; detect and handle AFTER triggers on update

	pxfres->Add(pexprDML);
}

// EOF
