//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarNullTest.h
//
//	@doc:
//		Scalar null test
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarNullTest_H
#define GPOPT_CScalarNullTest_H

#include "gpos/base.h"

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/operators/CScalar.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarNullTest
//
//	@doc:
//		Scalar null test operator
//
//---------------------------------------------------------------------------
class CScalarNullTest : public CScalar
{
private:
	// is nul or is not null operation
	BOOL m_is_null{true};

public:
	CScalarNullTest(const CScalarNullTest &) = delete;

	// ctor
	explicit CScalarNullTest(CMemoryPool *mp) : CScalar(mp)
	{
	}

	// ctor
	explicit CScalarNullTest(CMemoryPool *mp, BOOL is_null)
		: CScalar(mp), m_is_null(is_null)
	{
	}

	// dtor
	~CScalarNullTest() override = default;


	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopScalarNullTest;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CScalarNullTest";
	}

	// match function
	BOOL Matches(COperator *) const override;

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		return false;
	}

	// NullTest operator type
	BOOL IsNullTest() const;

	// return a copy of the operator with remapped columns
	COperator *
	PopCopyWithRemappedColumns(CMemoryPool *,		//mp,
							   UlongToColRefMap *,	//colref_mapping,
							   BOOL					//must_exist
							   ) override
	{
		return PopCopyDefault();
	}

	// the type of the scalar expression
	IMDId *MdidType() const override;

	// boolean expression evaluation
	EBoolEvalResult Eber(ULongPtrArray *pdrgpulChildren) const override;

	// conversion function
	static CScalarNullTest *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopScalarNullTest == pop->Eopid());

		return dynamic_cast<CScalarNullTest *>(pop);
	}

};	// class CScalarNullTest

}  // namespace gpopt


#endif	// !GPOPT_CScalarNullTest_H

// EOF
