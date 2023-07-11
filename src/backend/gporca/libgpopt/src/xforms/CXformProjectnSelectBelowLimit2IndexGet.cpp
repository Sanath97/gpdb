//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2023 VMware, Inc. or its affiliates.
//
//	@filename:
//		CXformLimit2IndexGet.cpp
//
//	@doc:
//		Transform LogicalGet in a limit to LogicalIndexGet if order by columns
//		match any of the index prefix
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformProjectnSelectBelowLimit2IndexGet.h"
#include "gpopt/xforms/CXformLimit2IndexGet.h"

#include "gpos/base.h"

#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CLogicalLimit.h"
#include "gpopt/operators/CLogicalProject.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPatternNode.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/CMDIndexGPDB.h"
#include "naucrates/md/CMDRelationGPDB.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CXformLimit2IndexGet::CXformLimit2IndexGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformProjectnSelectBelowLimit2IndexGet::
	CXformProjectnSelectBelowLimit2IndexGet(CMemoryPool *mp)
	:  // pattern
	  CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalLimit(mp),
			  GPOS_NEW(mp) CExpression(
				  mp,
				  GPOS_NEW(mp)
					  CPatternNode(mp, CPatternNode::EmtMatchProjectOrSelect),
				  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalGet(mp)),
				  GPOS_NEW(mp) CExpression(
					  mp,
					  GPOS_NEW(mp) CPatternTree(mp))),	// relational child
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(
											   mp)),  // scalar child for offset
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp)
						  CPatternLeaf(mp))	 // scalar child for number of rows
			  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLimit2IndexGet::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformProjectnSelectBelowLimit2IndexGet::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLimit2IndexGet::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformProjectnSelectBelowLimit2IndexGet::Transform(CXformContext *pxfctxt,
												   CXformResult *pxfres,
												   CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CLogicalLimit *popLimit = CLogicalLimit::PopConvert(pexpr->Pop());
	// extract components
	CExpression *pexprFirstChild = (*pexpr)[0];
	CExpression *pexprScalarOffset = (*pexpr)[1];
	CExpression *pexprScalarRows = (*pexpr)[2];
	CExpression *pexprLogGet = (*pexprFirstChild)[0];

	CLogicalGet *popGet = CLogicalGet::PopConvert(pexprLogGet->Pop());
	// get the indices count of this relation
	const ULONG ulIndices = popGet->Ptabdesc()->IndexCount();
	// Ignore xform if relation doesn't have any indices
	if (0 == ulIndices)
	{
		return;
	}

	CColRefArray *pdrgpcrIndexColumns = nullptr;
	CExpression *pexprUpdtRltn = nullptr;
	CColRefSet *pcrsScalarExpr = popLimit->PcrsLocalUsed();
	CExpressionArray *pdrgpexpr = nullptr;
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel =
		md_accessor->RetrieveRel(popGet->Ptabdesc()->MDId());

	if (COperator::EopLogicalSelect == pexprFirstChild->Pop()->Eopid()) {

		CExpression *pexprScalar = (*pexprFirstChild)[1];
		// array of expressions in the scalar expression
		pdrgpexpr =
			CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);
		GPOS_ASSERT(pdrgpexpr->Size() > 0);
		pexprUpdtRltn = pexprLogGet;
	}
	else {
		CExpression *boolConstExpr = nullptr;
		boolConstExpr = CUtils::PexprScalarConstBool(mp, true);
		pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
		pdrgpexpr->Append(boolConstExpr);
		popGet->AddRef();
		pexprUpdtRltn =
			GPOS_NEW(mp) CExpression(mp, popGet, boolConstExpr);
	}

	for (ULONG ul = 0; ul < ulIndices; ul++)
	{
		IMDId *pmdidIndex = pmdrel->IndexMDidAt(ul);
		const IMDIndex *pmdindex = md_accessor->RetrieveIndex(pmdidIndex);
		// get columns in the index
		pdrgpcrIndexColumns = CXformUtils::PdrgpcrIndexKeys(
			mp, popGet->PdrgpcrOutput(), pmdindex, pmdrel);
		COrderSpec* pos = popLimit->Pos();
		if (CXformLimit2IndexGet::FIndexApplicableForOrderBy(pos, pdrgpcrIndexColumns))
		{
			// build IndexGet expression
			CExpression *pexprIndexGet = CXformUtils::PexprLogicalIndexGet(
				mp, md_accessor, pexprUpdtRltn, popLimit->UlOpId(), pdrgpexpr,
				pcrsScalarExpr, nullptr /*outer_refs*/, pmdindex, pmdrel, true);

			if (pexprIndexGet != nullptr) {
				CExpression *pexprNewLimit = nullptr;
				CExpression *pUpdtRltnExpr = nullptr;
				if(COperator::EopLogicalProject == pexprFirstChild->Pop()->Eopid()){
					CLogicalProject *popProj = CLogicalProject::PopConvert(pexprFirstChild->Pop());
					CExpression *projectColsExpr = (*pexprFirstChild)[1];
					projectColsExpr->AddRef();
					pUpdtRltnExpr = GPOS_NEW(mp) CExpression(mp, popProj, pexprIndexGet, projectColsExpr);
				}
				else {
					pUpdtRltnExpr = pexprIndexGet;
				}
				pexprNewLimit =
					CXformLimit2IndexGet::PexprLimit(mp, pUpdtRltnExpr, pexprScalarOffset,
							   pexprScalarRows, popLimit->Pos(),
							   popLimit->FGlobal(),	 // fGlobal
							   popLimit->FHasCount(),
							   popLimit->IsTopLimitUnderDMLorCTAS());
				pxfres->Add(pexprNewLimit);
			}
		}
		pdrgpcrIndexColumns->Release();
	}
	pdrgpexpr->Release();
	if(COperator::EopLogicalProject == pexprFirstChild->Pop()->Eopid()){
		pexprUpdtRltn->Release();
	}
}
