//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2023 VMware, Inc. or its affiliates.
//
//	@filename:
//		CXformLimit2IndexGet.h
//
//	@doc:
//		Transform LogicalGet in a limit to LogicalIndexGet if order by columns
//		match any of the index prefix
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformProjectnSelectBelowLimit2IndexGet_H
#define GPOPT_CXformProjectnSelectBelowLimit2IndexGet_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"
#include "gpopt/operators/CLogicalGet.h"
namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLimit2IndexGet
//
//	@doc:
//		Transform LogicalGet in a limit to LogicalIndexGet if order by columns
//		match any of the index prefix
//---------------------------------------------------------------------------
class CXformProjectnSelectBelowLimit2IndexGet : public CXformExploration
{
public:
	CXformProjectnSelectBelowLimit2IndexGet(const CXformProjectnSelectBelowLimit2IndexGet &) = delete;
	// ctor
	explicit CXformProjectnSelectBelowLimit2IndexGet(CMemoryPool *mp);

	// dtor
	~CXformProjectnSelectBelowLimit2IndexGet() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfProjectnSelectBelowLimit2IndexGet;
	}

	// xform name
	const CHAR *
	SzId() const override
	{
		return "CXformProjectnSelectBelowLimit2IndexGet";
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;
} ;	// class CXformProjectnSelectBelowLimit2IndexGet

}  // namespace gpopt


#endif	//GPOPT_CXformProjectnSelectBelowLimit2IndexGet_H
