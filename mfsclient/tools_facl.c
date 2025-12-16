/*
 * Copyright (C) 2025 Jakub Kruszona-Zawadzki, Saglabs SA
 * 
 * This file is part of MooseFS.
 * 
 * MooseFS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2 (only).
 * 
 * MooseFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see
 * <https://www.gnu.org/licenses/>.
 */

#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <stdlib.h>
#include <inttypes.h>
#include <ctype.h>
#include <unistd.h>
#include <pwd.h>
#include <grp.h>

#include "libmfstools.h"

// #define ACL_DEBUG 1

typedef struct _acl_entry {
	uint32_t ugid;
	uint16_t perm;
} acl_entry;

typedef struct _acl_tab {
	uint16_t size;
	uint16_t cnt;
	acl_entry *data;
} acl_tab;

typedef struct _acl_node {
	uint8_t valid;
	uint8_t maskwaschanged;
	uint16_t userperm;
	uint16_t groupperm;
	uint16_t otherperm;
	uint16_t mask;
	acl_tab namedusers;
	acl_tab namedgroups;
} acl_node;

typedef struct _acl_data {
	uint32_t uid,gid;
	uint16_t mode;
	acl_node access_acl;
	acl_node default_acl;
} acl_data;

typedef struct _acl_command {
	uint8_t aclcmd;
	uint8_t acltype;
	uint32_t ugid;
	uint16_t perm;
	struct _acl_command *next;
} acl_command;

#define ACLCMD_REMOVE_ACCESS 1
#define ACLCMD_REMOVE_DEFAULT 2
#define ACLCMD_MODIFY_ACCESS 3
#define ACLCMD_MODIFY_DEFAULT 4
#define ACLCMD_REMOVEALL_ACCESS 5
#define ACLCMD_REMOVEALL_DEFAULT 6

#define ACLCMD_IS_DEFAULT(x) (((x)&1)==0)
#define ACLCMD_TO_DEFAULT(x) (ACLCMD_IS_DEFAULT(x)?(x):((x)+1))
#define ACLCMD_HAS_PERM(x) ((x)>=3 && (x)<=6)

#define ACLTYPE_USER 1
#define ACLTYPE_GROUP 2
#define ACLTYPE_MASK 3
#define ACLTYPE_OTHER 4

#define MAINID 0xFFFFFFFFU

#define DISPLAY_FLAG_ACCESS 1
#define DISPLAY_FLAG_DEFAULT 2
#define DISPLAY_FLAG_SKIPHEADER 4
#define DISPLAY_FLAG_ALLEFFECTIVE 8
#define DISPLAY_FLAG_NOEFFECTIVE 16
#define DISPLAY_FLAG_SKIPBASE 32
#define DISPLAY_FLAG_ABSOLUTE 64
#define DISPLAY_FLAG_NUMERIC 128

#define MAX_LINE_LENGTH 16384

#define	UGID_BUFF_SIZE 16384

#define MAX_NAME_LENGTH 256

void acltab_init(acl_tab *atab,uint32_t cnt) {
	if (atab->data!=NULL){
		free(atab->data);
	}
	if (cnt==0) {
		atab->size = 0;
		atab->cnt = 0;
		atab->data = NULL;
	} else {
		atab->size = (cnt+255)&0xFF00;
		atab->cnt = cnt;
		atab->data = malloc(sizeof(acl_entry)*atab->size);
		if (atab->data==NULL) {
			fprintf(stderr,"out of memory\n");
			exit(1);
		}
	}
}

void acltab_delete(acl_tab *atab,uint32_t ugid) {
	uint32_t i;
	for (i=0 ; i<atab->cnt ; i++) {
		if (atab->data[i].ugid == ugid) {
			atab->cnt--;
			if (i<atab->cnt) {
				atab->data[i].ugid = atab->data[atab->cnt].ugid;
				atab->data[i].perm = atab->data[atab->cnt].perm;
			}
			return;
		}
	}
}

void acltab_modify(acl_tab *atab,uint32_t ugid,uint16_t perm) {
	uint32_t i;
	for (i=0 ; i<atab->cnt ; i++) {
		if (atab->data[i].ugid == ugid) {
			atab->data[i].perm = perm;
			return;
		}
	}
	if (atab->cnt>=atab->size) {
		atab->size += 256;
		atab->data = realloc(atab->data,sizeof(acl_entry)*atab->size);
		if (atab->data==NULL) {
			fprintf(stderr,"out of memory\n");
			exit(1);
		}
	}
	i = atab->cnt;
	atab->data[i].ugid = ugid;
	atab->data[i].perm = perm;
	atab->cnt++;
}

void faclinitacn(acl_node *acn) {
	acn->valid = 0;
	acn->maskwaschanged = 0;
	acn->userperm = 0xFFFF;
	acn->groupperm = 0xFFFF;
	acn->otherperm = 0xFFFF;
	acn->mask = 0xFFFF;
	acltab_init(&(acn->namedusers),0);
	acltab_init(&(acn->namedgroups),0);
}

int getfileattr(const char *fname,uint32_t inode,acl_data *acl) {
	int32_t leng;

	ps_comminit();
	ps_put32(inode);
	ps_put8(0);
	ps_put32(geteuid());
	ps_put32(getegid());

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_GETATTR, MATOCL_FUSE_GETATTR, fname))<0) {
		return -1;
	}

	if (leng == 1) {
		fprintf(stderr,"%s: %s\n", fname, mfsstrerr(ps_get8()));
		return -1;
	}
	if (leng != ATTR_RECORD_SIZE && leng != ATTR_RECORD_SIZE-1) {
		fprintf(stderr,"%s: master query: wrong answer (leng): %d\n", fname, leng+4);
		return -1;
	}
	ps_dummyget(1);
	acl->mode = ps_get16();
	acl->uid = ps_get32();
	acl->gid = ps_get32();
	return 0;
}

int getfileacl(const char *fname,uint32_t inode,uint8_t acltype,acl_node *acn) {
	int32_t leng;
	uint16_t i,ucnt,gcnt;
	uint8_t status;

	faclinitacn(acn);

	ps_comminit();
	ps_put32(inode);
	ps_put8(acltype);

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_GETFACL, MATOCL_FUSE_GETFACL, fname))<0) {
		return -1;
	}

	if (leng == 1) {
		status = ps_get8();
		if (status==MFS_ERROR_ENOATTR) {
			return 0;
		}
		fprintf(stderr,"%s: %s\n", fname, mfsstrerr(ps_get8()));
		return -1;
	}

	acn->valid = 1;
	acn->userperm = ps_get16();
	acn->groupperm = ps_get16();
	acn->otherperm = ps_get16();
	acn->mask = ps_get16();
	ucnt = ps_get16();
	gcnt = ps_get16();
	acltab_init(&(acn->namedusers),ucnt);
	acltab_init(&(acn->namedgroups),gcnt);
	for (i=0 ; i<ucnt ; i++) {
		acn->namedusers.data[i].ugid = ps_get32();
		acn->namedusers.data[i].perm = ps_get16();
	}
	for (i=0 ; i<gcnt ; i++) {
		acn->namedgroups.data[i].ugid = ps_get32();
		acn->namedgroups.data[i].perm = ps_get16();
	}
	return 0;
}

int setfileacl(const char *fname,uint32_t inode,uint8_t acltype,acl_node *acn) {
	int32_t leng;
	uint16_t i;
	uint8_t status;

	ps_comminit();
	ps_put32(inode);
	ps_put32(geteuid());
	ps_put8(acltype);

	ps_put16(acn->userperm);
	ps_put16(acn->groupperm);
	ps_put16(acn->otherperm);
	ps_put16(acn->mask);
	ps_put16(acn->namedusers.cnt);
	ps_put16(acn->namedgroups.cnt);
	for (i=0 ; i<acn->namedusers.cnt ; i++) {
		ps_put32(acn->namedusers.data[i].ugid);
		ps_put16(acn->namedusers.data[i].perm);
	}
	for (i=0 ; i<acn->namedgroups.cnt ; i++) {
		ps_put32(acn->namedgroups.data[i].ugid);
		ps_put16(acn->namedgroups.data[i].perm);
	}

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_SETFACL, MATOCL_FUSE_SETFACL, fname))<0) {
		return -1;
	}

	if (leng!=1) {
		fprintf(stderr,"%s: master query: wrong answer (leng): %d\n", fname, leng+4);
		return -1;
	}

	status = ps_get8();
	if (status!=MFS_STATUS_OK) {
		fprintf(stderr,"%s: %s\n", fname, mfsstrerr(status));
		return -1;
	}
	return 0;
}

int print_uid(uint32_t uid, uint8_t idasnum) {
	struct passwd *user=NULL;

	if (idasnum || !(user = getpwuid(uid))) {
		return printf("%"PRIu32, uid);
	} else {
		return printf("%s", user->pw_name);
	}
}

int print_gid(uint32_t gid, uint8_t idasnum) {
	struct group *group=NULL;

	if (idasnum || !(group = getgrgid(gid))) {
		return printf("%"PRIu32, gid);
	} else {
		return printf("%s", group->gr_name);
	}
}

int print_perm(uint16_t perm) {
	if (perm==0xFFFF) {
		return printf("(deleted)");
	} else if (perm>7) {
		return printf("%04X",perm);
	} else {
		return printf("%c%c%c",(perm&4)?'r':'-',(perm&2)?'w':'-',(perm&1)?'x':'-');
	}
}

void print_effective(int pos,uint16_t baseperm,uint16_t mask,uint8_t display_flags) {
	if (display_flags & DISPLAY_FLAG_NOEFFECTIVE) {
		return;
	}
	if ((display_flags & DISPLAY_FLAG_ALLEFFECTIVE) || ((baseperm & mask) != baseperm)) {
		while (pos<31) {
			putchar(' ');
			pos++;
		}
		printf(" #effective:");
		print_perm(baseperm & mask);
	}
}

void printaclfrommode(acl_data *acl) {
	if (acl->access_acl.valid==0) {
		printf("user::");
		print_perm((acl->mode>>6)&0x7);
		printf("\n");
		printf("group::");
		print_perm((acl->mode>>3)&0x7);
		printf("\n");
		printf("other::");
		print_perm((acl->mode)&0x7);
		printf("\n");
	}
}

void showaclnode(const char *prefix,acl_node *acn,uint8_t display_flags) {
	uint16_t i;
	int pos;

	if (acn->valid) {
		printf("%suser::",prefix);
		print_perm(acn->userperm);
	//	print_effective(acn->userperm,acn->mask,display_flags);
		printf("\n");
		for (i=0 ; i<acn->namedusers.cnt ; i++) {
			pos = printf("%suser:",prefix);
			pos += print_uid(acn->namedusers.data[i].ugid,display_flags & DISPLAY_FLAG_NUMERIC);
			pos += printf(":");
			pos += print_perm(acn->namedusers.data[i].perm);
			print_effective(pos,acn->namedusers.data[i].perm,acn->mask,display_flags);
			printf("\n");
		}
		pos = printf("%sgroup::",prefix);
		pos += print_perm(acn->groupperm);
		print_effective(pos,acn->groupperm,acn->mask,display_flags);
		printf("\n");
		for (i=0 ; i<acn->namedgroups.cnt ; i++) {
			pos = printf("%sgroup:",prefix);
			pos += print_gid(acn->namedgroups.data[i].ugid,display_flags & DISPLAY_FLAG_NUMERIC);
			pos += printf(":");
			pos += print_perm(acn->namedgroups.data[i].perm);
			print_effective(pos,acn->namedgroups.data[i].perm,acn->mask,display_flags);
			printf("\n");
		}
		if (acn->mask!=0xFFFF) {
			printf("%smask::",prefix);
			print_perm(acn->mask);
			printf("\n");
		}
		printf("%sother::",prefix);
		print_perm(acn->otherperm);
		printf("\n");
	}
}

void showfileacl(const char *fname,acl_data *acl,uint8_t display_flags) {
	const char *p;
	uint16_t perm;

	if (display_flags & DISPLAY_FLAG_SKIPBASE) {
		if (acl->access_acl.valid==0 && acl->default_acl.valid==0) {
			return;
		}
	}

	if ((display_flags & DISPLAY_FLAG_SKIPHEADER)==0) {
		if (fname[0]=='/' && (display_flags & DISPLAY_FLAG_ABSOLUTE)==0) {
			p = fname;
			while (*p=='/') {
				p++;
			}
		} else {
			p = fname;
		}
		printf("# file: %s\n",p);
		printf("# owner: ");
		print_uid(acl->uid,display_flags & DISPLAY_FLAG_NUMERIC);
		printf("\n");
		printf("# group: ");
		print_gid(acl->gid,display_flags & DISPLAY_FLAG_NUMERIC);
		printf("\n");
		perm = (acl->mode>>9)&7;
		if (perm!=0) {
			printf("# flags: %c%c%c\n",(perm&4)?'s':'-',(perm&2)?'s':'-',(perm&2)?'t':'-');
		}
	}

	switch (display_flags & (DISPLAY_FLAG_DEFAULT|DISPLAY_FLAG_ACCESS)) {
		case 0:
		case (DISPLAY_FLAG_DEFAULT|DISPLAY_FLAG_ACCESS):
			printaclfrommode(acl);
			showaclnode("",&(acl->access_acl),display_flags);
			showaclnode("default:",&(acl->default_acl),display_flags);
			break;
		case DISPLAY_FLAG_ACCESS:
			printaclfrommode(acl);
			showaclnode("",&(acl->access_acl),display_flags);
			break;
		case DISPLAY_FLAG_DEFAULT:
			showaclnode("",&(acl->default_acl),display_flags);
			break;
	}

	printf("\n");
}

void debug_print_acn(acl_node *acn) {
	uint32_t i;
	printf("    valid: %u\n",acn->valid);
	printf("    maskwaschanged: %u\n",acn->maskwaschanged);
	printf("    userperm: %04"PRIX16"\n",acn->userperm);
	printf("    groupperm: %04"PRIX16"\n",acn->groupperm);
	printf("    otherperm: %04"PRIX16"\n",acn->otherperm);
	printf("    mask: %04"PRIX16"\n",acn->mask);
	for (i=0 ; i<acn->namedusers.cnt ; i++) {
		printf("    * nameduser: %"PRIu32" ; perm: %04"PRIX16"\n",acn->namedusers.data[i].ugid,acn->namedusers.data[i].perm);
	}
	for (i=0 ; i<acn->namedgroups.cnt ; i++) {
		printf("    * namedgroup: %"PRIu32" ; perm: %04"PRIX16"\n",acn->namedgroups.data[i].ugid,acn->namedgroups.data[i].perm);
	}
}

void debug_print_acl(acl_data *acl) {
	printf("  uid: %"PRIu32"\n",acl->uid);
	printf("  gid: %"PRIu32"\n",acl->gid);
	printf("  mode: 0%"PRIo16"\n",acl->mode);
	printf("  access:\n");
	debug_print_acn(&(acl->access_acl));
	printf("  default:\n");
	debug_print_acn(&(acl->default_acl));
}

void debug_print_command(acl_command *cmd) {
	switch(cmd->aclcmd) {
		case ACLCMD_REMOVE_ACCESS:
			printf("REMOVE_ACCESS");
			break;
		case ACLCMD_REMOVE_DEFAULT:
			printf("REMOVE_DEFAULT");
			break;
		case ACLCMD_MODIFY_ACCESS:
			printf("MODIFY_ACCESS");
			break;
		case ACLCMD_MODIFY_DEFAULT:
			printf("MODIFY_DEFAULT");
			break;
		case ACLCMD_REMOVEALL_ACCESS:
			printf("REMOVEALL_ACCESS");
			break;
		case ACLCMD_REMOVEALL_DEFAULT:
			printf("REMOVEALL_DEFAULT");
			break;
		default:
			printf("UNKNOWN_ACLCMD");
	}
	printf(":");
	switch(cmd->acltype) {
		case ACLTYPE_USER:
			printf("USER");
			break;
		case ACLTYPE_GROUP:
			printf("GROUP");
			break;
		case ACLTYPE_MASK:
			printf("MASK");
			break;
		case ACLTYPE_OTHER:
			printf("OTHER");
			break;
		default:
			if (cmd->aclcmd!=ACLCMD_REMOVEALL_ACCESS && cmd->aclcmd!=ACLCMD_REMOVEALL_DEFAULT) {
				printf("UNKNOWN_ACLTYPE");
			}
	}
	printf(":");
	if (cmd->ugid!=MAINID) {
		printf("%"PRIu32,cmd->ugid);
	}
	printf(":");
	print_perm(cmd->perm);
	printf("\n");
}

#define	UGID_BUFF_SIZE 16384

int parse_uid(uint32_t *ugid,const char *name,uint8_t silent) {
	char ugidbuff[UGID_BUFF_SIZE];
	char uname[MAX_NAME_LENGTH];
	int i;
	uint8_t onlydigits;
	struct passwd pwd,*result;

	onlydigits = 1;
	i = 0;
	while (*name && *name!=':' && *name!=',' && i<MAX_NAME_LENGTH) {
		uname[i] = *name;
		if (*name<'0' || *name>'9') {
			onlydigits = 0;
		}
		name++;
		i++;
	}
	if (i==MAX_NAME_LENGTH) {
		if (silent==0) {
			fprintf(stderr,"user name too long\n");
		}
		return -1;
	}
	uname[i]=0;

	if (getpwnam_r(uname, &pwd, ugidbuff, UGID_BUFF_SIZE, &result) != 0 || !result) {
		if (onlydigits) {
			*ugid = strtoul(uname,NULL,10);
			return i;
		}
		if (silent==0) {
			fprintf(stderr,"can't find user: %s\n",uname);
		}
		return -1;
	}
	*ugid = pwd.pw_uid;
	return i;
}

int parse_gid(uint32_t *ugid,const char *name) {
	char ugidbuff[UGID_BUFF_SIZE];
	char gname[MAX_NAME_LENGTH];
	int i;
	uint8_t onlydigits;
	struct group grp,*result;

	onlydigits = 1;
	i = 0;
	while (*name && *name!=':' && *name!=',' && i<MAX_NAME_LENGTH) {
		gname[i] = *name;
		if (*name<'0' || *name>'9') {
			onlydigits = 0;
		}
		name++;
		i++;
	}
	if (i==MAX_NAME_LENGTH) {
		fprintf(stderr,"group name too long\n");
		return -1;
	}
	gname[i]=0;

	if (getgrnam_r(gname, &grp, ugidbuff, UGID_BUFF_SIZE, &result) != 0 || !result) {
		if (onlydigits) {
			*ugid = strtoul(gname,NULL,10);
			return i;
		}
		fprintf(stderr,"can't find group: %s\n",gname);
		return -1;
	}
	*ugid = grp.gr_gid;
	return i;
}

void faclnewcmd(acl_command ***tail,uint8_t aclcmd,uint8_t acltype,uint32_t ugid,uint16_t perm) {
	acl_command *cmd;
	cmd = malloc(sizeof(acl_command));
	cmd->aclcmd = aclcmd;
	cmd->acltype = acltype;
	cmd->ugid = ugid;
	cmd->perm = perm;
	cmd->next = NULL;
	**tail = cmd;
	*tail = &(cmd->next);
}

int faclparsestr(acl_command ***tail,uint8_t aclcmd,const char *permstr) {
	char c;
	uint8_t aclcmd_toadd;
	uint8_t acltype;
	uint32_t ugid;
	uint16_t perm;
	uint8_t state;
	int i;

	aclcmd_toadd = aclcmd;
	acltype = 0;
	ugid = MAINID;
	perm = 0;
	state = 0;
	while ((c=*permstr)!=0) {
		if (c=='#') {
			break;
		} else if (c==' ' || c=='\t' || c=='\r' || c=='\n') {
			permstr++;
		} else if (c==',') {
			if (state==3 || (!ACLCMD_HAS_PERM(aclcmd) && state==2)) {
				faclnewcmd(tail,aclcmd_toadd,acltype,ugid,perm);
			} else {
				fprintf(stderr,"incomplete ACL definition (encountered before: %s)\n",permstr);
				return -1;
			}
			permstr++;
			aclcmd_toadd = aclcmd;
			acltype = 0;
			ugid = MAINID;
			perm = 0;
			state = 0;
		} else if (c=='d' && state==0 && !ACLCMD_IS_DEFAULT(aclcmd_toadd)) {
			if (permstr[1]==':') {
				permstr+=2;
				aclcmd_toadd = ACLCMD_TO_DEFAULT(aclcmd_toadd);
				state = 1;
			} else if (strncmp(permstr+1,"efault:",7)==0) {
				permstr+=8;
				aclcmd_toadd = ACLCMD_TO_DEFAULT(aclcmd_toadd);
				state = 1;
			} else if ((i=parse_uid(&ugid,permstr,1))>0) {
				permstr+=i;
				acltype = ACLTYPE_USER;
				state = 2;
				continue;
			} else {
				fprintf(stderr,"perm parse error (%s)\n",permstr);
				return -1;
			}
		} else if (c=='u' && state<=1) {
			if (permstr[1]==':') {
				permstr+=2;
			} else if (strncmp(permstr+1,"ser:",4)==0) {
				permstr+=5;
			} else if ((i=parse_uid(&ugid,permstr,1))>0) {
				permstr+=i;
				acltype = ACLTYPE_USER;
				state = 2;
				continue;
			} else {
				fprintf(stderr,"perm parse error (%s)\n",permstr);
				return -1;
			}
			if (*permstr==':') {
				permstr++;
				acltype = ACLTYPE_USER;
				state = 3;
			} else {
				i = parse_uid(&ugid,permstr,0);
				if (i<0) {
					return -1;
				}
				permstr+=i;
				acltype = ACLTYPE_USER;
				state = 2;
			}
		} else if (c=='g' && state<=1) {
			if (permstr[1]==':') {
				permstr+=2;
			} else if (strncmp(permstr+1,"roup:",5)==0) {
				permstr+=6;
			} else if ((i=parse_uid(&ugid,permstr,1))>0) {
				permstr+=i;
				acltype = ACLTYPE_USER;
				state = 2;
				continue;
			} else {
				fprintf(stderr,"perm parse error (%s)\n",permstr);
				return -1;
			}
			if (*permstr==':') {
				permstr++;
				acltype = ACLTYPE_GROUP;
				state = 3;
			} else {
				i = parse_gid(&ugid,permstr);
				if (i<0) {
					return -1;
				}
				permstr+=i;
				acltype = ACLTYPE_GROUP;
				state = 2;
			}
		} else if (c=='m' && state<=1) {
			if (permstr[1]==':') {
				permstr+=2;
			} else if (strncmp(permstr+1,"ask:",4)==0) {
				permstr+=5;
			} else if ((i=parse_uid(&ugid,permstr,1))>0) {
				permstr+=i;
				acltype = ACLTYPE_USER;
				state = 2;
				continue;
			} else {
				fprintf(stderr,"perm parse error (%s)\n",permstr);
				return -1;
			}
			acltype = ACLTYPE_MASK;
			state = 2;
		} else if (c=='o' && state<=1) {
			if (permstr[1]==':') {
				permstr+=2;
			} else if (strncmp(permstr+1,"ther:",5)==0) {
				permstr+=6;
			} else if ((i=parse_uid(&ugid,permstr,1))>0) {
				permstr+=i;
				acltype = ACLTYPE_USER;
				state = 2;
				continue;
			} else {
				fprintf(stderr,"perm parse error (%s)\n",permstr);
				return -1;
			}
			acltype = ACLTYPE_OTHER;
			state = 2;
		} else if (state<=1) {
			if ((i=parse_uid(&ugid,permstr,1))>0) {
				permstr+=i;
				acltype = ACLTYPE_USER;
				state = 2;
				continue;
			} else {
				fprintf(stderr,"perm parse error (%s)\n",permstr);
				return -1;
			}
		} else if (c==':' && state==2) {
			state = 3;
			permstr++;
		} else if (c=='r' && state>1 && (perm&4)==0 && ACLCMD_HAS_PERM(aclcmd)) {
			permstr++;
			perm |= 4;
			state = 3;
		} else if (c=='w' && state>1 && (perm&2)==0 && ACLCMD_HAS_PERM(aclcmd)) {
			permstr++;
			perm |= 2;
			state = 3;
		} else if (c=='x' && state>1 && (perm&1)==0 && ACLCMD_HAS_PERM(aclcmd)) {
			permstr++;
			perm |= 1;
			state = 3;
		} else if (c>='0' && c<='7' && perm==0 && state>1 && ACLCMD_HAS_PERM(aclcmd)) {
			permstr++;
			perm = c-'0';
			state = 3;
		} else if (c=='-' && state>1 && ACLCMD_HAS_PERM(aclcmd)) {
			permstr++;
			state = 3;
		} else {
			fprintf(stderr,"perm parse error (%s)\n",permstr);
			return -1;
		}
	}
	if (state==3 || (!ACLCMD_HAS_PERM(aclcmd) && state==2)) {
		faclnewcmd(tail,aclcmd_toadd,acltype,ugid,perm);
	} else if (state>0) {
		fprintf(stderr,"incomplete ACL definition (at the end of string)\n");
		return -1;
	}
	return 0;
}

int faclparsefile(acl_command ***tail,uint8_t aclcmd,const char *fname) {
	FILE *fd;
	char *lbuff;
	size_t lbsize;
	int lineno;

	fd = fopen(fname,"r");
	if (fd==NULL) {
		fprintf(stderr,"can't open file: %s\n",fname);
		return -1;
	}

	lbsize = MAX_LINE_LENGTH;
	lbuff = malloc(lbsize);
	lineno = 1;
	while (getline(&lbuff,&lbsize,fd)!=-1) {
		if (faclparsestr(tail,aclcmd,lbuff)<0) {
			fprintf(stderr,"%s: parse error in line %u\n",fname,lineno);
			fclose(fd);
			return -1;
		}
		lineno++;
	}

	free(lbuff);
	fclose(fd);
	return 0;
}

int faclapplycmd(acl_data *acl,acl_command *cmd) {
	uint16_t force_group;
	acl_node *acn;
	acl_tab *atab;
	if (ACLCMD_IS_DEFAULT(cmd->aclcmd)) {
		acn = &(acl->default_acl);
	} else {
		acn = &(acl->access_acl);
	}
	switch (cmd->aclcmd) {
		case ACLCMD_REMOVE_ACCESS:
		case ACLCMD_REMOVE_DEFAULT:
//			if (cmd->aclcmd==ACLCMD_REMOVE_ACCESS && (cmd->ugid==MAINID || cmd->acltype==ACLTYPE_MASK || cmd->acltype==ACLTYPE_OTHER)) {
//				fprintf(stderr,"can't remove basic access attributes\n");
//				return -1;
//			}
			if (cmd->ugid==MAINID) {
				if (cmd->acltype==ACLTYPE_USER) {
					acn->userperm = 0xFFFF;
				} else if (cmd->acltype==ACLTYPE_GROUP) {
					acn->groupperm = 0xFFFF;
				} else if (cmd->acltype==ACLTYPE_OTHER) {
					acn->otherperm = 0xFFFF;
				} else if (cmd->acltype==ACLTYPE_MASK) {
					acn->mask = 0xFFFF;
				}
			} else {
				if (cmd->acltype==ACLTYPE_USER) {
					atab = &(acn->namedusers);
				} else if (cmd->acltype==ACLTYPE_GROUP) {
					atab = &(acn->namedgroups);
				} else { // internal error !!!
					return -1;
				}
				acltab_delete(atab,cmd->ugid);
			}
			break;
		case ACLCMD_MODIFY_ACCESS:
		case ACLCMD_MODIFY_DEFAULT:
			if (cmd->ugid==MAINID) {
				if (cmd->acltype==ACLTYPE_USER) {
					acn->userperm = cmd->perm;
				} else if (cmd->acltype==ACLTYPE_GROUP) {
					acn->groupperm = cmd->perm;
				} else if (cmd->acltype==ACLTYPE_OTHER) {
					acn->otherperm = cmd->perm;
				} else if (cmd->acltype==ACLTYPE_MASK) {
					acn->mask = cmd->perm;
					acn->maskwaschanged = 1;
				} else { // internal error !!!
					return -1;
				}
			} else {
				if (cmd->acltype==ACLTYPE_USER) {
					atab = &(acn->namedusers);
				} else if (cmd->acltype==ACLTYPE_GROUP) {
					atab = &(acn->namedgroups);
				} else { // internal error !!!
					return -1;
				}
				acltab_modify(atab,cmd->ugid,cmd->perm);
			}
			if (cmd->aclcmd==ACLCMD_MODIFY_ACCESS) {
				if (acn->userperm==0xFFFF) {
					acn->userperm = (acl->mode>>6) & 7;
				}
				if (acn->groupperm==0xFFFF) {
					acn->groupperm = (acl->mode>>3) & 7;
				}
				if (acn->otherperm==0xFFFF) {
					acn->otherperm = (acl->mode) & 7;
				}
				if (acn->mask==0xFFFF) {
					acn->mask = acn->groupperm;
				}
			}
			acn->valid = 1;
			break;
		case ACLCMD_REMOVEALL_ACCESS:
		case ACLCMD_REMOVEALL_DEFAULT:
			force_group = 0xFFFFU;
			if (cmd->aclcmd==ACLCMD_REMOVEALL_ACCESS && acn->valid && acn->mask!=0xFFFF) {
				if (acn->groupperm==0xFFFF) {
//					if (((acl->mode>>3) & 7 & acn->mask) != ((acl->mode>>3) & 7)) {
					force_group = (acl->mode>>3) & 7 & acn->mask;
//					}
				} else {
//					if ((acn->groupperm & acn->mask) != acn->groupperm) {
					force_group = acn->groupperm & acn->mask;
//					}
				}
			}
			faclinitacn(acn);
			if (force_group!=0xFFFFU) {
				acn->groupperm = force_group;
				acn->valid = 1;
			}
			break;
	}
	return 0;
}

void faclrecalcmask(acl_node *acn,uint8_t no_mask_flag,uint16_t mode) {
	uint16_t i;
	uint8_t perm;
	if (acn->maskwaschanged || acn->valid==0) {
		return;
	}
	if (acn->namedusers.cnt==0 && acn->namedgroups.cnt==0 && acn->mask==0xFFFF) {
		return;
	}
	if (no_mask_flag && acn->mask==0xFFFF) {
		acn->mask = (mode>>3)&0x7;
		return;
	}
	if (acn->groupperm==0xFFFF) {
		perm = (mode>>3)&0x7;
	} else {
		perm = acn->groupperm;
	}
	for (i=0 ; i<acn->namedusers.cnt ; i++) {
		perm |= acn->namedusers.data[i].perm;
	}
	for (i=0 ; i<acn->namedgroups.cnt ; i++) {
		perm |= acn->namedgroups.data[i].perm;
	}
	acn->mask = perm;
}

/*
void faclfixacn(acl_node *acn) {
	if (acn->namedusers.cnt==0 && acn->namedgroups.cnt==0 && acn->mask==0xFFFF) {
		faclinitacn(acn);
	}
}
*/

int faclapplyall(acl_data *acl,acl_command *cmd,uint8_t no_mask_flag) {
	while (cmd!=NULL) {
#ifdef ACL_DEBUG
		printf("\n");
		debug_print_command(cmd);
#endif
		if (faclapplycmd(acl,cmd)<0) {
			return -1;
		}
#ifdef ACL_DEBUG
		printf("\n");
		debug_print_acl(acl);
#endif
		cmd = cmd->next;
	}
//	faclfixacn(&(acl->access_acl)); // in access we need to have non-empty lists or defined mask
	faclrecalcmask(&(acl->access_acl),no_mask_flag,acl->mode);
//	if (acl->default_acl.mask!=0xFFFF) {
	faclrecalcmask(&(acl->default_acl),no_mask_flag,acl->mode);
//	}
	return 0;
}

int get_facl(const char *fname,acl_data *acl) {
	uint32_t inode;

	if (open_master_conn(fname, &inode, NULL, NULL, 0, 0) < 0) {
		return -1;
	}

	if (getfileattr(fname,inode,acl)<0) {
		goto error;
	}

	if (getfileacl(fname,inode,POSIX_ACL_ACCESS,&(acl->access_acl))<0) {
		goto error;
	}

	if (getfileacl(fname,inode,POSIX_ACL_DEFAULT,&(acl->default_acl))<0) {
		goto error;
	}

	return 0;
error:
	reset_master();
	return -1;
}

int set_facl(const char *fname,acl_data *acl) {
	uint32_t inode;

	if (open_master_conn(fname, &inode, NULL, NULL, 0, 1)<0) {
		return -1;
	}

	if (setfileacl(fname,inode,POSIX_ACL_ACCESS,&(acl->access_acl))<0) {
		goto error;
	}

	if (setfileacl(fname,inode,POSIX_ACL_DEFAULT,&(acl->default_acl))<0) {
		goto error;
	}

	return 0;
error:
	reset_master();
	return -1;

}

//----------------------------------------------------------------------

static const char *getfacltxt[] = {
	"get objects access control list (posix acl)",
	"",
	"usage: "_EXENAME_" [-?] [-adceEspn] object [object ...]",
	"",
	_QMARKDESC_,
	" -a - display the file access control list only",
	" -d - display the default access control list only",
	" -c - do not display the comment header",
	" -e - print all effective rights",
	" -E - print no effective rights",
	" -s - skip files that only have the base entries",
	" -p - don't strip leading '/' in pathnames",
	" -n - print numeric user/group identifiers",
	NULL
};

static const char *setfacltxt[] = {
	"set objects access control list (posix acl)",
	"",
	"usage: "_EXENAME_" [-?] [-bKknd] [{-s|-m|-x} acl_spec] [{-S|-M|-X} acl_file] object [object ...]",
	"",
	_QMARKDESC_,
	" -b - remove all extended ACL entries",
	" -K - remove the access ACL",
	" -k - remove the default ACL",
	" -n - don't recalculate the effective rights mask",
	" -d - operations apply to the default ACL",
	" -s - set the ACL of file(s), replacing the current ACL",
	" -S - read ACL entries to set from file",
	" -m - modify the current ACL(s) of file(s)",
	" -M - read ACL entries to modify from file",
	" -x - remove entries from the ACL(s) of file(s)",
	" -X - read ACL entries to remove from file",
	NULL
};

void getfaclusage(void) {
	tcomm_print_help(getfacltxt);
	exit(1);
}

void setfaclusage(void) {
	tcomm_print_help(setfacltxt);
	exit(1);
}

int getfaclexe(int argc,char *argv[]) {
	acl_data acl;
	uint8_t display_flags;
	int ch;
	int status = 0;

	memset(&acl,0,sizeof(acl_data));
	display_flags = 0;
	while ((ch=getopt(argc,argv,"?adceEspn"))!=-1) {
		switch (ch) {
			case 'a':
				display_flags |= DISPLAY_FLAG_ACCESS;
				break;
			case 'd':
				display_flags |= DISPLAY_FLAG_DEFAULT;
				break;
			case 'c':
				display_flags |= DISPLAY_FLAG_SKIPHEADER;
				break;
			case 'e':
				display_flags |= DISPLAY_FLAG_ALLEFFECTIVE;
				break;
			case 'E':
				display_flags |= DISPLAY_FLAG_NOEFFECTIVE;
				break;
			case 's':
				display_flags |= DISPLAY_FLAG_SKIPBASE;
				break;
			case 'p':
				display_flags |= DISPLAY_FLAG_ABSOLUTE;
				break;
			case 'n':
				display_flags |= DISPLAY_FLAG_NUMERIC;
				break;
			default:
				getfaclusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc==0) {
		getfaclusage();
		return 1;
	}

	while (argc>0) {
		if (get_facl(*argv,&acl)<0) {
			status = 1;
		} else {
			showfileacl(*argv,&acl,display_flags);
		}
		argc--;
		argv++;
	}

	return status;
}

int setfaclexe(int argc,char *argv[]) {
	acl_data acl;
	acl_command *aclcommands;
	acl_command **aclcmdtail;
	uint8_t default_flag;
	uint8_t no_mask_flag;
	int ch;
	int status = 0;

	aclcommands = NULL;
	aclcmdtail = &(aclcommands);
	default_flag = 0;
	no_mask_flag = 0;
	memset(&acl,0,sizeof(acl_data));

	while ((ch=getopt(argc,argv,"?bKknds:S:m:M:x:X:"))!=-1) {
		switch (ch) {
			case 'b':
				faclnewcmd(&aclcmdtail,ACLCMD_REMOVEALL_ACCESS,0,MAINID,0);
				faclnewcmd(&aclcmdtail,ACLCMD_REMOVEALL_DEFAULT,0,MAINID,0);
				break;
			case 'K':
				faclnewcmd(&aclcmdtail,ACLCMD_REMOVEALL_ACCESS,0,MAINID,0);
				break;
			case 'k':
				faclnewcmd(&aclcmdtail,ACLCMD_REMOVEALL_DEFAULT,0,MAINID,0);
				break;
			case 'n':
				no_mask_flag = 1;
				break;
			case 'd':
				default_flag = 1;
				break;
			case 's':
				faclnewcmd(&aclcmdtail,default_flag?ACLCMD_REMOVEALL_DEFAULT:ACLCMD_REMOVEALL_ACCESS,0,MAINID,0);
				faclparsestr(&aclcmdtail,default_flag?ACLCMD_MODIFY_DEFAULT:ACLCMD_MODIFY_ACCESS,optarg);
				break;
			case 'S':
				faclnewcmd(&aclcmdtail,default_flag?ACLCMD_REMOVEALL_DEFAULT:ACLCMD_REMOVEALL_ACCESS,0,MAINID,0);
				faclparsefile(&aclcmdtail,default_flag?ACLCMD_MODIFY_DEFAULT:ACLCMD_MODIFY_ACCESS,optarg);
				break;
			case 'm':
				faclparsestr(&aclcmdtail,default_flag?ACLCMD_MODIFY_DEFAULT:ACLCMD_MODIFY_ACCESS,optarg);
				break;
			case 'M':
				faclparsefile(&aclcmdtail,default_flag?ACLCMD_MODIFY_DEFAULT:ACLCMD_MODIFY_ACCESS,optarg);
				break;
			case 'x':
				faclparsestr(&aclcmdtail,default_flag?ACLCMD_REMOVE_DEFAULT:ACLCMD_REMOVE_ACCESS,optarg);
				break;
			case 'X':
				faclparsefile(&aclcmdtail,default_flag?ACLCMD_REMOVE_DEFAULT:ACLCMD_REMOVE_ACCESS,optarg);
				break;
			default:
				setfaclusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc==0) {
		setfaclusage();
		return 1;
	}

#ifdef ACL_DEBUG
	if (no_mask_flag) {
	 	printf("do not recalculate mask\n");
	}
	printf("\n");
#endif

	while (argc>0) {
		// ACL debug
#ifdef ACL_DEBUG
		get_facl(*argv,&acl);
		showfileacl(*argv,&acl,0);
		debug_print_acl(&acl);
		faclapplyall(&acl,aclcommands,no_mask_flag);
		showfileacl(*argv,&acl,0);
		debug_print_acl(&acl);
		set_facl(*argv,&acl);
		get_facl(*argv,&acl);
		showfileacl(*argv,&acl,0);
#else
		if (get_facl(*argv,&acl)<0) {
			status = 1;
		} else if (faclapplyall(&acl,aclcommands,no_mask_flag)<0) {
			status = 1;
		} else if (set_facl(*argv,&acl)<0) {
			status = 1;
		}
#endif
		argc--;
		argv++;
	}

	return status;
}



static command commandlist[] = {
	{"get", getfaclexe},
	{"set", setfaclexe},
	{NULL,NULL}
};

int main(int argc,char *argv[]) {
	return tcomm_find_and_execute(argc,argv,commandlist);
}
