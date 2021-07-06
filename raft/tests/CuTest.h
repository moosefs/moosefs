#ifndef CU_TEST_H
#define CU_TEST_H

#include <setjmp.h>
#include <stdarg.h>

/* CuString */

char* CuStrAlloc(int size);
char* CuStrCopy(const char* old);

#define CU_ALLOC(TYPE)		((TYPE*) malloc(sizeof(TYPE)))

#define HUGE_STRING_LEN	8192
#define STRING_MAX		256
#define STRING_INC		256

typedef struct
{
	int length;
	int size;
	char* buffer;
} CuString;

void CuStringInit(CuString* str);
CuString* CuStringNew(void);
void CuStringRead(CuString* str, const char* path);
void CuStringAppend(CuString* str, const char* text);
void CuStringAppendChar(CuString* str, char ch);
void CuStringAppendFormat(CuString* str, const char* format, ...);
void CuStringInsert(CuString* str, const char* text, int pos);
void CuStringResize(CuString* str, int newSize);

/* CuTest */

typedef struct CuTest CuTest;

typedef void (*TestFunction)(CuTest *);

struct CuTest
{
	const char* name;
	TestFunction function;
	int failed;
	int ran;
	const char* message;
	jmp_buf *jumpBuf;
};

void CuTestInit(CuTest* t, const char* name, TestFunction function);
CuTest* CuTestNew(const char* name, TestFunction function);
void CuTestRun(CuTest* tc);

/* Internal versions of assert functions -- use the public versions */
void CuFail_Line(CuTest* tc, const char* file, int line, const char* message2, const char* message);
void CuAssert_Line(CuTest* tc, const char* file, int line, const char* message, int condition);
void CuAssertStrEquals_LineMsg(CuTest* tc, 
	const char* file, int line, const char* message, 
	const char* expected, const char* actual);
void CuAssertIntEquals_LineMsg(CuTest* tc, 
	const char* file, int line, const char* message, 
	int expected, int actual);
void CuAssertDblEquals_LineMsg(CuTest* tc, 
	const char* file, int line, const char* message, 
	double expected, double actual, double delta);
void CuAssertPtrEquals_LineMsg(CuTest* tc, 
	const char* file, int line, const char* message, 
	void* expected, void* actual);

/* public assert functions */

#define CuFail(tc, ms)                        CuFail_Line(  (tc), __FILE__, __LINE__, NULL, (ms))
#define CuAssert(tc, ms, cond)                CuAssert_Line((tc), __FILE__, __LINE__, (ms), (cond))
#define CuAssertTrue(tc, cond)                CuAssert_Line((tc), __FILE__, __LINE__, "assert failed", (cond))

#define CuAssertStrEquals(tc,ex,ac)           CuAssertStrEquals_LineMsg((tc),__FILE__,__LINE__,NULL,(ex),(ac))
#define CuAssertStrEquals_Msg(tc,ms,ex,ac)    CuAssertStrEquals_LineMsg((tc),__FILE__,__LINE__,(ms),(ex),(ac))
#define CuAssertIntEquals(tc,ex,ac)           CuAssertIntEquals_LineMsg((tc),__FILE__,__LINE__,NULL,(ex),(ac))
#define CuAssertIntEquals_Msg(tc,ms,ex,ac)    CuAssertIntEquals_LineMsg((tc),__FILE__,__LINE__,(ms),(ex),(ac))
#define CuAssertDblEquals(tc,ex,ac,dl)        CuAssertDblEquals_LineMsg((tc),__FILE__,__LINE__,NULL,(ex),(ac),(dl))
#define CuAssertDblEquals_Msg(tc,ms,ex,ac,dl) CuAssertDblEquals_LineMsg((tc),__FILE__,__LINE__,(ms),(ex),(ac),(dl))
#define CuAssertPtrEquals(tc,ex,ac)           CuAssertPtrEquals_LineMsg((tc),__FILE__,__LINE__,NULL,(ex),(ac))
#define CuAssertPtrEquals_Msg(tc,ms,ex,ac)    CuAssertPtrEquals_LineMsg((tc),__FILE__,__LINE__,(ms),(ex),(ac))

#define CuAssertPtrNotNull(tc,p)        CuAssert_Line((tc),__FILE__,__LINE__,"null pointer unexpected",(p != NULL))
#define CuAssertPtrNotNullMsg(tc,msg,p) CuAssert_Line((tc),__FILE__,__LINE__,(msg),(p != NULL))

/* CuSuite */

#define MAX_TEST_CASES	1024

#define SUITE_ADD_TEST(SUITE,TEST)	CuSuiteAdd(SUITE, CuTestNew(#TEST, TEST))

typedef struct
{
	int count;
	CuTest* list[MAX_TEST_CASES];
	int failCount;

} CuSuite;


void CuSuiteInit(CuSuite* testSuite);
CuSuite* CuSuiteNew(void);
void CuSuiteAdd(CuSuite* testSuite, CuTest *testCase);
void CuSuiteAddSuite(CuSuite* testSuite, CuSuite* testSuite2);
void CuSuiteRun(CuSuite* testSuite);
void CuSuiteSummary(CuSuite* testSuite, CuString* summary);
void CuSuiteDetails(CuSuite* testSuite, CuString* details);

#endif /* CU_TEST_H */
