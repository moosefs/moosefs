try:
    from common.constants_ac import *
except:
	pass # imports may be unaccessible in a single mfscli file but they should be already inlined instead

# some constants from MFSCommunication.h
ANTOAN_NOP = 0

ANTOCS_CLEAR_ERRORS        = (PROTO_BASE+306)
CSTOAN_CLEAR_ERRORS        = (PROTO_BASE+307)

CLTOMA_CSERV_LIST          = (PROTO_BASE+500)
MATOCL_CSERV_LIST          = (PROTO_BASE+501)
CLTOAN_CHART_DATA          = (PROTO_BASE+506)
ANTOCL_CHART_DATA          = (PROTO_BASE+507)
CLTOMA_SESSION_LIST        = (PROTO_BASE+508)
MATOCL_SESSION_LIST        = (PROTO_BASE+509)
CLTOMA_INFO                = (PROTO_BASE+510)
MATOCL_INFO                = (PROTO_BASE+511)
CLTOMA_FSTEST_INFO         = (PROTO_BASE+512)
MATOCL_FSTEST_INFO         = (PROTO_BASE+513)
CLTOMA_CHUNKSTEST_INFO     = (PROTO_BASE+514)
MATOCL_CHUNKSTEST_INFO     = (PROTO_BASE+515)
CLTOMA_CHUNKS_MATRIX       = (PROTO_BASE+516)
MATOCL_CHUNKS_MATRIX       = (PROTO_BASE+517)
CLTOMA_QUOTA_INFO          = (PROTO_BASE+518)
MATOCL_QUOTA_INFO          = (PROTO_BASE+519)
CLTOMA_EXPORTS_INFO        = (PROTO_BASE+520)
MATOCL_EXPORTS_INFO        = (PROTO_BASE+521)
CLTOMA_MLOG_LIST           = (PROTO_BASE+522)
MATOCL_MLOG_LIST           = (PROTO_BASE+523)
CLTOMA_CSSERV_COMMAND      = (PROTO_BASE+524)
MATOCL_CSSERV_COMMAND      = (PROTO_BASE+525)
CLTOMA_SESSION_COMMAND     = (PROTO_BASE+526)
MATOCL_SESSION_COMMAND     = (PROTO_BASE+527)
CLTOMA_MEMORY_INFO         = (PROTO_BASE+528)
MATOCL_MEMORY_INFO         = (PROTO_BASE+529)
CLTOMA_LIST_OPEN_FILES     = (PROTO_BASE+532)
MATOCL_LIST_OPEN_FILES     = (PROTO_BASE+533)
CLTOMA_LIST_ACQUIRED_LOCKS = (PROTO_BASE+534)
MATOCL_LIST_ACQUIRED_LOCKS = (PROTO_BASE+535)
CLTOMA_MASS_RESOLVE_PATHS  = (PROTO_BASE+536)
MATOCL_MASS_RESOLVE_PATHS  = (PROTO_BASE+537)
CLTOMA_SCLASS_INFO         = (PROTO_BASE+542)
MATOCL_SCLASS_INFO         = (PROTO_BASE+543)
CLTOMA_MISSING_CHUNKS      = (PROTO_BASE+544)
MATOCL_MISSING_CHUNKS      = (PROTO_BASE+545)
CLTOMA_PATTERN_INFO        = (PROTO_BASE+548)
MATOCL_PATTERN_INFO        = (PROTO_BASE+549)
CLTOMA_INSTANCE_NAME       = (PROTO_BASE+550)
MATOCL_INSTANCE_NAME       = (PROTO_BASE+551)

CLTOCS_HDD_LIST            = (PROTO_BASE+600)
CSTOCL_HDD_LIST            = (PROTO_BASE+601)
MFS_MESSAGE = 1

FEATURE_EXPORT_UMASK        = 0
FEATURE_EXPORT_DISABLES     = 1
FEATURE_SESSION_STATS_28    = 2
FEATURE_INSTANCE_NAME       = 4
FEATURE_CSLIST_MODE         = 5
FEATURE_SCLASS_IN_MATRIX    = 7
FEATURE_DEFAULT_GRACEPERIOD = 8
FEATURE_LABELMODE_OVERRIDES = 9
FEATURE_SCLASSGROUPS        = 10

MASKORGROUP = 4
SCLASS_EXPR_MAX_SIZE = 128

MFS_CSSERV_COMMAND_REMOVE         = 0
MFS_CSSERV_COMMAND_BACKTOWORK     = 1
MFS_CSSERV_COMMAND_MAINTENANCEON  = 2
MFS_CSSERV_COMMAND_MAINTENANCEOFF = 3
MFS_CSSERV_COMMAND_TMPREMOVE      = 4

MFS_SESSION_COMMAND_REMOVE = 0

PATTERN_EUGID_ANY            = 0xFFFFFFFF
PATTERN_OMASK_SCLASS         = 0x01
PATTERN_OMASK_TRASHRETENTION = 0x02
PATTERN_OMASK_EATTR          = 0x04

SCLASS_ARCH_MODE_CTIME      = 0x01
SCLASS_ARCH_MODE_MTIME      = 0x02
SCLASS_ARCH_MODE_ATIME      = 0x04
SCLASS_ARCH_MODE_REVERSIBLE = 0x08
SCLASS_ARCH_MODE_FAST       = 0x10
SCLASS_ARCH_MODE_CHUNK      = 0x20

STATUS_OK      = 0
ERROR_NOTFOUND = 41
ERROR_ACTIVE   = 42

STATE_UNREACHABLE = 0xFFFF
STATE_DUMMY       = 0x00
STATE_LEADER      = 0x01
STATE_ELECT       = 0x02
STATE_FOLLOWER    = 0x03
STATE_USURPER     = 0x04
STATE_DEPUTY      = 0x05
STATE_MASTERCE    = 0xFF
STATE_STR_UNREACHABLE = "UNREACHABLE"
STATE_STR_BUSY        = "BUSY"

#CSTOCL_HDD_LIST flags constants
CS_HDD_MFR      = 0x01
CS_HDD_DAMAGED  = 0x02
CS_HDD_SCANNING = 0x04
CS_HDD_INVALID  = 0x08

MFRSTATUS_VALIDATING = 0 # unknown after disconnect or creation or unknown, loop in progress
MFRSTATUS_INPROGRESS = 1 # chunks still needs to be replicated, can't be removed 
MFRSTATUS_READY      = 2 # can be removed, whole loop has passed

CS_HDD_CS_VALID       = 0 #valid hdd data from CS
CS_HDD_CS_TOO_OLD     = 1 #too old CS to get info on its discs
CS_HDD_CS_UNREACHABLE = 2 #can't connect to CS and get info on its discs

CS_LOAD_NORMAL	       = 0
CS_LOAD_OVERLOADED     = 1
CS_LOAD_REBALANCE      = 2
CS_LOAD_FAST_REBALANCE = 3
CS_LOAD_GRACEFUL       = 4

LASTSTORE_UNKNOWN		 = 0 # if laststore_ts==0
LASTSTORE_META_STORED_BG = 0 # Stored in background if laststore_ts>0
LASTSTORE_DOWNLOADED     = 1 # Downloaded
LASTSTORE_META_STORED_FG = 2 # Stored in foreground
LASTSTORE_CRC_STORED_BG  = 3 # CRC stored in background

MISSING_CHUNK_TYPE_NOCOPY         = 0
MISSING_CHUNK_TYPE_INVALID_COPIES = 1
MISSING_CHUNK_TYPE_WRONG_VERSIONS = 2
MISSING_CHUNK_TYPE_PARTIAL_EC     = 3

FSTEST_INFO_STR_LIMIT = 100000 # 100k limit of datastr in MATOCL_FSTEST_INFO

UNRESOLVED = "(unresolved)"

TRESHOLD_WARNING = 0.90
TRESHOLD_ERROR   = 0.95

LICVER_CE = -1 # Community Edition licver value

AJAX_NONE      = None
AJAX_CONTAINER = 1
AJAX_METRICS   = 2

# Chunk matrix summary colums
MX_COL_MISSING       = 0
MX_COL_ENDANGERED    = 1
MX_COL_UNDERGOAL     = 2
MX_COL_STABLE        = 3
MX_COL_OVERGOAL      = 4
MX_COL_DELETEPENDING = 5
MX_COL_DELETEREADY   = 6
MX_COL_TOTAL         = 7

MFS_LOCK_TYPE_UNKNOWN   = 0
MFS_LOCK_TYPE_SHARED		= 1
MFS_LOCK_TYPE_EXCLUSIVE	= 2