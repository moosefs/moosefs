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
 * along with MooseFS; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/param.h>

#include "libmfstools.h"

#define DEFAULT_PATH "/mnt/mfs"

typedef struct _storage_class {
	uint8_t dleng;
	uint8_t desc[MAXSCLASSDESCLENG];
	uint32_t priority;
	uint8_t export_group;
	uint8_t admin_only;
	uint8_t labels_mode;
	uint8_t arch_mode;
	uint16_t arch_delay;
	uint16_t min_trashretention;
	uint64_t arch_min_size;
	parser_data create;
	parser_data keep;
	parser_data arch;
	parser_data trash;
} storage_class;

static inline int deserialize_sc_new(uint8_t protocol, int32_t leng, storage_class *sc) {
	uint8_t i;
	int32_t header_leng;

	header_leng = (protocol>=6)?47:(protocol>=5)?41:(protocol>=4)?37:29;

	memset(sc, 0, sizeof(storage_class));
	if (leng<header_leng) {		//not enough data for the mandatory fields
		return -1;
	}
	if (protocol>=6) {
		sc->dleng = ps_get8();
		if (leng<header_leng+sc->dleng) {
			return -1;
		}
		if (sc->dleng>0) {
			ps_getbytes(sc->desc,sc->dleng);
		}
		sc->priority = ps_get32();
		sc->export_group = ps_get8();
	} else {
		sc->dleng = 0;
		sc->priority = 0;
		sc->export_group = 0;
	}
	sc->admin_only = ps_get8();
	sc->labels_mode = ps_get8();
	sc->arch_mode = ps_get8();
	sc->arch_delay = ps_get16();
	if (protocol>=4) {
		sc->arch_min_size = ps_get64();
	} else {
		sc->arch_min_size = 0;
	}
	sc->min_trashretention = ps_get16();
	sc->arch.ec_data_chksum_parts = ps_get8();
	sc->trash.ec_data_chksum_parts = ps_get8();
	if (protocol>=5) {
		sc->create.labels_mode = ps_get8();
		sc->keep.labels_mode = ps_get8();
		sc->arch.labels_mode = ps_get8();
		sc->trash.labels_mode = ps_get8();
	} else {
		sc->create.labels_mode = LABELS_MODE_GLOBAL;
		sc->keep.labels_mode = LABELS_MODE_GLOBAL;
		sc->arch.labels_mode = LABELS_MODE_GLOBAL;
		sc->trash.labels_mode = LABELS_MODE_GLOBAL;
	}
	sc->create.uniqmask = ps_get32();
	sc->keep.uniqmask = ps_get32();
	sc->arch.uniqmask = ps_get32();
	sc->trash.uniqmask = ps_get32();
	sc->create.labelscnt = ps_get8();
	sc->keep.labelscnt = ps_get8();
	sc->arch.labelscnt = ps_get8();
	sc->trash.labelscnt = ps_get8();

	if (leng<(header_leng+sc->dleng+(sc->create.labelscnt+sc->keep.labelscnt+sc->arch.labelscnt+sc->trash.labelscnt)*SCLASS_EXPR_MAX_SIZE)) {
		return -1;
	}
	if ((sc->create.labelscnt>9)\
	 ||((sc->keep.labelscnt>9 || sc->keep.labelscnt<1))\
	 || (sc->arch.labelscnt>9)\
	 || (sc->trash.labelscnt>9)
	) {
		return -1;
	}
	for (i=0; i<sc->create.labelscnt; i++) {
		ps_getbytes(sc->create.labelexpr[i],SCLASS_EXPR_MAX_SIZE);
	}
	for (i=0; i<sc->keep.labelscnt; i++) {
		ps_getbytes(sc->keep.labelexpr[i], SCLASS_EXPR_MAX_SIZE);
	}
	for (i=0; i<sc->arch.labelscnt; i++) {
		ps_getbytes(sc->arch.labelexpr[i], SCLASS_EXPR_MAX_SIZE);
	}
	for (i=0 ; i<sc->trash.labelscnt ; i++) {
		ps_getbytes(sc->trash.labelexpr[i], SCLASS_EXPR_MAX_SIZE);
	}
//ok:
	return header_leng+sc->dleng+(sc->create.labelscnt+sc->keep.labelscnt+sc->arch.labelscnt+sc->trash.labelscnt)*SCLASS_EXPR_MAX_SIZE;
}

static inline uint32_t serialize_sc_new(uint8_t protocol,const storage_class *sc) {
	uint8_t i;
	int32_t header_leng;

	header_leng = (protocol>=6)?(47+sc->dleng):(protocol>=5)?41:(protocol>=4)?37:29;

	if (protocol>=6) {
		ps_put8(sc->dleng);
		ps_putbytes(sc->desc,sc->dleng);
		ps_put32(sc->priority);
		ps_put8(sc->export_group);
	}
	ps_put8 (sc->admin_only);
	ps_put8 (sc->labels_mode);
	ps_put8 (sc->arch_mode);
	ps_put16(sc->arch_delay);
	if (protocol>=4) {
		ps_put64(sc->arch_min_size);
	}
	ps_put16(sc->min_trashretention);
	ps_put8 (sc->arch.ec_data_chksum_parts);
	ps_put8 (sc->trash.ec_data_chksum_parts);
	if (protocol>=5) {
		ps_put8(sc->create.labels_mode);
		ps_put8(sc->keep.labels_mode);
		ps_put8(sc->arch.labels_mode);
		ps_put8(sc->trash.labels_mode);
	}
	ps_put32(sc->create.uniqmask);
	ps_put32(sc->keep.uniqmask);
	ps_put32(sc->arch.uniqmask);
	ps_put32(sc->trash.uniqmask);
	ps_put8(sc->create.labelscnt);
	ps_put8(sc->keep.labelscnt);
	ps_put8(sc->arch.labelscnt);
	ps_put8(sc->trash.labelscnt);

	for (i=0 ; i<sc->create.labelscnt ; i++) {
		ps_putbytes(sc->create.labelexpr[i], SCLASS_EXPR_MAX_SIZE);
	}
	for (i=0 ; i<sc->keep.labelscnt ; i++) {
		ps_putbytes(sc->keep.labelexpr[i], SCLASS_EXPR_MAX_SIZE);
	}
	for (i=0 ; i<sc->arch.labelscnt ; i++) {
		ps_putbytes(sc->arch.labelexpr[i], SCLASS_EXPR_MAX_SIZE);
	}
	for (i=0 ; i<sc->trash.labelscnt ; i++) {
		ps_putbytes(sc->trash.labelexpr[i], SCLASS_EXPR_MAX_SIZE);
	}
	return header_leng+(sc->create.labelscnt+sc->keep.labelscnt+sc->arch.labelscnt+sc->trash.labelscnt)*SCLASS_EXPR_MAX_SIZE;
}

int name_validate(const char *name) {
	uint8_t c,space_flag;
	if (name[0]==' ') {
		return -1;
	}
	space_flag = 0;
	while (*name) {
		c = *name;
		if (c<32) {
			return -1;
		} else if (c==32) {
			space_flag = 1;
		} else {
			space_flag = 0;
		}
		name++;
	}
	if (space_flag) {
		return -1;
	}
	return 0;
}

int names_validate(int argc,char *argv[]) {
	while (argc>0) {
		if (name_validate(*argv)<0) {
			return -1;
		}
		argc--;
		argv++;
	}
	return 0;
}

void printf_time(uint16_t h) {
	if (h>=24) {
		printf("%ud",h/24);
		h = h%24;
		if (h) {
			printf(" %uh",h);
		}
	} else if (h>0) {
		printf("%uh",h);
	} else {
		printf("0");
	}
}

int parse_time(const char **p,uint16_t *h) {
	uint16_t cnt;
	char c;
	const char *ip = *p;

	cnt = 0;
	while (*ip>='0' && *ip<='9') {
		c = *ip;
		ip++;
		cnt *= 10;
		cnt += c-'0';
	}
	if (cnt==0 && (*ip==' ' || *ip=='\0')) {
		*h = 0;
		*p = ip;
		return 0;
	}
	if (*ip=='h') {
		*h = cnt;
		*p = ip+1;
		return 0;
	}
	if (*ip=='d') {
		*h = cnt * 24;
		ip++;
		if (ip[0]==' ' && ip[1]>='0' && ip[1]<='9') {
			ip++;
			cnt = 0;
			while (*ip>='0' && *ip<='9') {
				c = *ip;
				ip++;
				cnt *= 10;
				cnt += c-'0';
			}
			if (*ip=='h') {
				*h += cnt;
				*p = ip+1;
				return 0;
			}
		} else {
			*p = ip;
			return 0;
		}
	}
	return -1;
}

const char *arch_mode_descr[] = {
	"",
	"C",
	"M",
	"MC",
	"A",
	"AC",
	"AM",
	"AMC",
	"R",
	"RC",
	"RM",
	"RMC",
	"RA",
	"RAC",
	"RAM",
	"RAMC"
};

void printf_sc(const storage_class *sc, const char *scname, uint8_t classid, char *endstr) {
	int i;
	char labelsbuff[LABELS_BUFF_SIZE];

	if (scname != NULL) {
		printf("name: %s\n", scname);
	} else {
		printf("\n");
	}

	if (classid>0) {
		printf("\tclassid: %u\n",classid);
	}
	if (sc->dleng>0) {
		printf("\tdescription: ");
		for (i=0 ; i<sc->dleng ; i++) {
			putchar(sc->desc[i]);
		}
		printf("\n");
	}
	printf("\tpriority: %"PRIu32" ; export_group: %"PRIu8"\n",sc->priority,sc->export_group);
	printf("\tadmin_only: %s ; labels_mode: %s ; ",
		(sc->admin_only) ? "YES" : "NO",
		(sc->labels_mode==LABELS_MODE_LOOSE) ? "LOOSE" : (sc->labels_mode==LABELS_MODE_STRICT) ? "STRICT" : "STD");
	printf("arch_mode: %s\n",
		(sc->arch_mode & SCLASS_ARCH_MODE_CHUNK) ? "P" : (sc->arch_mode & SCLASS_ARCH_MODE_FAST) ? "F" : arch_mode_descr[sc->arch_mode&0x0f]);

	printf("\tcreate_labels: %s\n", (sc->create.labelscnt) ? make_label_expr(labelsbuff,&(sc->create)) : "not defined - using keep settings");

	printf("\tkeep_labels: %s\n", make_label_expr(labelsbuff,&(sc->keep)));

	printf("\tarch_labels: %s", (sc->arch.labelscnt || sc->arch.ec_data_chksum_parts) ? make_label_expr(labelsbuff,&(sc->arch)) : "not defined - using keep settings");

	if ((sc->arch.labelscnt || sc->arch.ec_data_chksum_parts)) {
		if (sc->arch_delay && (sc->arch_mode & SCLASS_ARCH_MODE_FAST)==0) {
			printf(" ; arch_delay: ");
			printf_time(sc->arch_delay);
		}
		if (sc->arch_min_size>0) {
			printf(" ; arch_min_size: %"PRIu64,sc->arch_min_size);
		}
	}
	printf("\n");

	printf("\ttrash_labels: %s", (sc->trash.labelscnt || sc->trash.ec_data_chksum_parts) ? make_label_expr(labelsbuff, &(sc->trash)) : "not defined - using keep/arch settings");
	if (sc->trash.labelscnt || sc->trash.ec_data_chksum_parts) {
		printf(" ; min_trashretention: ");
		printf_time(sc->min_trashretention);
	}
	printf("\n");

	printf("%s",endstr);
}


// used in admin
int make_sc(const char *mfsmp, const char *scname, storage_class *sc) {
	uint32_t masterversion;
	uint8_t protocol;
	int32_t leng;
	uint32_t nleng;
	char cwdbuff[MAXPATHLEN];

	nleng = strlen(scname);
	if (nleng >= MAXSCLASSNAMELENG) {
		fprintf(stderr,"%s: name too long\n",scname);
		return -1;
	}
	if (mfsmp == NULL) {
		mfsmp = getcwd(cwdbuff,MAXPATHLEN);
	}
	if (open_master_conn(mfsmp, NULL, NULL, NULL, 0, 0) < 0) {
		fprintf(stderr,"trying default path (%s)\n",DEFAULT_PATH);
		if (open_master_conn(DEFAULT_PATH, NULL, NULL, NULL, 0, 0) < 0) {
			return -1;
		}
	}

	masterversion = getmasterversion();

	if (masterversion<VERSION2INT(4,2,0)) {
		fprintf(stderr,"master too old, this tool needs master 4.2.0 or newer\n");
		goto error;
	}

	protocol = (masterversion<VERSION2INT(4,34,2))?3:(masterversion<VERSION2INT(4,53,0))?4:(masterversion<VERSION2INT(4,57,0))?5:6;

	ps_comminit();

	ps_put8(nleng);
	ps_putbytes((const uint8_t *)scname, nleng);

	ps_put8(protocol);
	serialize_sc_new(protocol,sc);

	if ((leng = ps_send_and_receive(CLTOMA_SCLASS_CREATE, MATOCL_SCLASS_CREATE, NULL)) < 0) {
		goto error;
	}

	if (leng != 1) {
		fprintf(stderr,"master query: wrong answer (leng)\n");
		goto error;
	}
	if ((leng=ps_get8()) != MFS_STATUS_OK) {
		fprintf(stderr,"storage class make %s: error: %s\n",scname,mfsstrerr(leng));
		goto error;
	}
//OK:
	printf("storage class make %s: ok\n",scname);
	return 0;

error:
	reset_master();
	return -1;
}


int change_sc(const char *mfsmp, const char *scname, uint16_t chgmask, storage_class *sc) {
	uint32_t masterversion;
	uint8_t protocol;
	int32_t leng;
	uint32_t nleng;
	char cwdbuff[MAXPATHLEN];

	nleng=strlen(scname);
	if (nleng >= MAXSCLASSNAMELENG) {
		fprintf(stderr,"%s: name too long\n",scname);
		return -1;
	}
	if (mfsmp == NULL) {
		mfsmp = getcwd(cwdbuff, MAXPATHLEN);
	}
	if (open_master_conn(mfsmp, NULL, NULL, NULL, 0, 0) < 0) {
		fprintf(stderr,"trying default path (%s)\n",DEFAULT_PATH);
		if (open_master_conn(DEFAULT_PATH, NULL, NULL, NULL, 0, 0) < 0) {
			return -1;
		}
	}

	masterversion = getmasterversion();

	if (masterversion<VERSION2INT(4,2,0)) {
		fprintf(stderr,"master too old, this tool needs master 4.2.0 or newer\n");
		goto error;
	}

	protocol = (masterversion<VERSION2INT(4,34,2))?3:(masterversion<VERSION2INT(4,53,0))?4:(masterversion<VERSION2INT(4,57,0))?5:6;

	ps_comminit();

	ps_put8(nleng);
	ps_putbytes((uint8_t*)scname,nleng);

	ps_put8(protocol);
	ps_put16(chgmask);
	serialize_sc_new(protocol,sc);
	if ((leng = ps_send_and_receive(CLTOMA_SCLASS_CHANGE, MATOCL_SCLASS_CHANGE, NULL))<0) {
		goto error;
	}

	if (leng==0) {
		fprintf(stderr,"master query: wrong answer (leng)\n");
		goto error;
	}
	if (leng==1) {
		fprintf(stderr,"storage class %s %s: error: %s\n", chgmask ? "change" : "show", scname, mfsstrerr(ps_get8()));
		goto error;
	}
	if (ps_get8() != protocol) {
		fprintf(stderr,"master query: wrong answer (wrong data format)\n");
		goto error;
	}
	if (deserialize_sc_new(protocol, leng-1, sc) < 0) {
		fprintf(stderr,"master query: wrong answer (deserialize stoage class)\n");
		goto error;
	}
//OK
	printf("storage class changed: ");
	printf_sc(sc, scname, 0, "\n");
	return 0;

error:
	reset_master();
	return -1;
}


int remove_sc(const char *mfsmp, const char *scname) {
	int32_t leng;
	uint32_t nleng;
	char cwdbuff[MAXPATHLEN];

	nleng = strlen(scname);
	if (nleng >= MAXSCLASSNAMELENG) {
		fprintf(stderr,"%s: name too long\n",scname);
		return -1;
	}
	if (mfsmp == NULL) {
		mfsmp = getcwd(cwdbuff,MAXPATHLEN);
	}
	if (open_master_conn(mfsmp, NULL, NULL, NULL, 0, 0) < 0) {
		fprintf(stderr,"trying default path (%s)\n",DEFAULT_PATH);
		if (open_master_conn(DEFAULT_PATH, NULL, NULL, NULL, 0, 0) < 0) {
			return -1;
		}
	}

	ps_comminit();
	ps_put8(nleng);
	ps_putbytes((uint8_t*)scname,nleng);

	if ((leng = ps_send_and_receive(CLTOMA_SCLASS_DELETE, MATOCL_SCLASS_DELETE, NULL)) != 1) {
		fprintf(stderr,"master query: wrong answer (leng)\n");
		goto error;
	}

	uint8_t status;
	if ((status=ps_get8()) != MFS_STATUS_OK) {
		fprintf(stderr,"storage class remove %s: error: %s\n", scname, mfsstrerr(status));
		goto error;
	}
//OK
	printf("storage class remove %s: ok\n", scname);
	return 0;

error:
	reset_master();
	return -1;
}

int clone_sc(const char *mfsmp, const char *oldscname, const char *newscname) {
	uint32_t leng;
	uint32_t onleng,nnleng;
	char cwdbuff[MAXPATHLEN];

	onleng = strlen(oldscname);
	nnleng = strlen(newscname);
	if (onleng >= MAXSCLASSNAMELENG) {
		fprintf(stderr,"%s: name too long\n", oldscname);
		return -1;
	}
	if (nnleng >= MAXSCLASSNAMELENG) {
		fprintf(stderr,"%s: name too long\n", newscname);
		return -1;
	}
	if (mfsmp == NULL) {
		mfsmp = getcwd(cwdbuff,MAXPATHLEN);
	}
	if (open_master_conn(mfsmp, NULL, NULL, NULL, 0, 0) < 0) {
		fprintf(stderr,"trying default path (%s)\n",DEFAULT_PATH);
		if (open_master_conn(DEFAULT_PATH, NULL, NULL, NULL, 0, 0) < 0) {
			return -1;
		}
	}

	ps_comminit();
	ps_put8(onleng);
	ps_putbytes((const uint8_t*)oldscname,onleng);
	ps_put8(nnleng);
	ps_putbytes((const uint8_t*)newscname,nnleng);

	if ((leng = ps_send_and_receive(CLTOMA_SCLASS_DUPLICATE, MATOCL_SCLASS_DUPLICATE, NULL)) != 1) {
		fprintf(stderr,"master query: wrong answer (leng)\n");
		goto error;
	}

	uint8_t status;
	if ((status=ps_get8()) != MFS_STATUS_OK) {
		fprintf(stderr,"storage class clone %s->%s: error: %s\n", oldscname, newscname, mfsstrerr(status));
		goto error;
	}
//OK
	printf("storage class clone %s->%s: ok\n", oldscname, newscname);
	return 0;

error:
	reset_master();
	return -1;
}


int move_sc(const char *mfsmp, const char *oldscname, const char *newscname) {
	uint32_t leng;
	uint32_t onleng,nnleng;
	char cwdbuff[MAXPATHLEN];

	onleng = strlen(oldscname);
	nnleng = strlen(newscname);
	if (onleng >= MAXSCLASSNAMELENG) {
		fprintf(stderr,"%s: name too long\n", oldscname);
		return -1;
	}
	if (nnleng >= MAXSCLASSNAMELENG) {
		fprintf(stderr,"%s: name too long\n", newscname);
		return -1;
	}
	if (mfsmp == NULL) {
		mfsmp = getcwd(cwdbuff,MAXPATHLEN);
	}
	if (open_master_conn(mfsmp, NULL, NULL, NULL, 0, 0) < 0) {
		fprintf(stderr,"trying default path (%s)\n",DEFAULT_PATH);
		if (open_master_conn(DEFAULT_PATH, NULL, NULL, NULL, 0, 0) < 0) {
			return -1;
		}
	}

	ps_comminit();
	ps_put8(onleng);
	ps_putbytes((const uint8_t*)oldscname,onleng);
	ps_put8(nnleng);
	ps_putbytes((const uint8_t*)newscname,nnleng);

	if ((leng = ps_send_and_receive(CLTOMA_SCLASS_RENAME, MATOCL_SCLASS_RENAME, NULL)) != 1) {
		fprintf(stderr,"master query: wrong answer (leng)\n");
		goto error;
	}

	uint8_t status;
	if ((status=ps_get8()) != MFS_STATUS_OK) {
		fprintf(stderr,"storage class move %s->%s: error: %s\n", oldscname, newscname, mfsstrerr(status));
		goto error;
	}
//OK
	printf("storage class move %s->%s: ok\n", oldscname, newscname);
	return 0;

error:
	reset_master();
	return -1;
}


int list_sc(const char *mfsmp, uint8_t longmode, uint8_t insens, int argc, char *argv[]) {
	uint32_t masterversion;
	char scname[MAXSCLASSNAMELENG];
	int32_t leng;
	int dret, scn=0;
	storage_class sc;
	char cwdbuff[MAXPATHLEN];
	uint8_t nleng, n, name_match;
	uint8_t protocol,classid;

	if (mfsmp == NULL) {
		mfsmp = getcwd(cwdbuff, MAXPATHLEN);
	}
	if (open_master_conn(mfsmp, NULL, NULL, NULL, 0, 0) < 0) {
		fprintf(stderr,"trying default path (%s)\n",DEFAULT_PATH);
		if (open_master_conn(DEFAULT_PATH, NULL, NULL, NULL, 0, 0) < 0) {
			return -1;
		}
	}

	masterversion = getmasterversion();

	if (longmode && masterversion<VERSION2INT(4,2,0)) {
		fprintf(stderr,"long mode not supported (master too old)\n");
		goto error;
	}

	protocol = (masterversion<VERSION2INT(4,34,2))?3:(masterversion<VERSION2INT(4,53,0))?4:(masterversion<VERSION2INT(4,57,0))?5:6;

	ps_comminit();
	ps_put8(longmode ? protocol : 0); // 0,3,4,5,6 - requested packet version
	if ((leng = ps_send_and_receive(CLTOMA_SCLASS_LIST, MATOCL_SCLASS_LIST, NULL)) < 0) {
		return -1;
	}

	if (leng==1) {
		fprintf(stderr,"storage class list: error: %s\n", mfsstrerr(ps_get8()));
		goto error;
	}

	while (leng>0) {
		if (longmode && protocol>=4) {
			classid = ps_get8();
			leng--;
		} else {
			classid = 0;
		}
		nleng = ps_get8();
		leng--;
		if (nleng<=leng) {		//convert {strLen[8b],text} to  C-str "scname"
			ps_getbytes((uint8_t *)scname, nleng);
			leng -= nleng;
			scname[nleng]=0;	//C-str terminator
		}
		if (argc) {
			name_match=0;
			for (n=0; n<argc && !name_match; n++) {
				if (!mystrmatch(argv[n], scname, insens)) {
					scn++;
//					printf("#%-2d: ",++scn);
					name_match=1;
					break;
				}
			}
		} else {
			name_match=1;
			scn++;
//			printf("#%-2d: ",++scn);
		}
		if (longmode) {
			dret = deserialize_sc_new(protocol, leng, &sc);
			if (dret<0) {
				fprintf(stderr,"master query: wrong answer (deserialize storage class)\n");
				goto error;
			}
			leng -= dret;
			if (name_match) {
				printf_sc(&sc, scname, classid, "\n");
			}
		} else {
			if (name_match) {
				printf("%s\n", scname);
			}
		}
	}
//OK
	if (!scn) {
		printf("No matching class found.\n");
	}
	return 0;

error:
	reset_master();
	return -1;

}



int make_or_change_sc(const char *mfsmp,const char *scname,storage_class *sc,uint8_t overwrite) {
	// debug:
	// printf_sc(sc,scname,0,"\n");
	uint32_t masterversion;
	uint8_t protocol;
	uint8_t mfsstatus;
	int32_t leng;
	uint32_t nleng;
	char cwdbuff[MAXPATHLEN];

	nleng = strlen(scname);
	if (nleng >= MAXSCLASSNAMELENG) {
		printf("%s: name too long\n",scname);
		return -1;
	}
	if (mfsmp == NULL) {
		mfsmp = getcwd(cwdbuff,MAXPATHLEN);
	}
	if (open_master_conn(mfsmp, NULL, NULL, NULL, 0, 0) < 0) {
		fprintf(stderr,"trying default path (%s)\n",DEFAULT_PATH);
		if (open_master_conn(DEFAULT_PATH, NULL, NULL, NULL, 0, 0) < 0) {
			return -1;
		}
	}

	masterversion = getmasterversion();

	if (masterversion<VERSION2INT(4,2,0)) {
		fprintf(stderr,"master too old, this tool needs master 4.2.0 or newer\n");
		goto error;
	}

	protocol = (masterversion<VERSION2INT(4,34,2))?3:(masterversion<VERSION2INT(4,53,0))?4:(masterversion<VERSION2INT(4,57,0))?5:6;

	ps_comminit();

	ps_put8(nleng);
	ps_putbytes((const uint8_t *)scname, nleng);

	ps_put8(protocol);
	serialize_sc_new(protocol,sc);

	if ((leng = ps_send_and_receive(CLTOMA_SCLASS_CREATE, MATOCL_SCLASS_CREATE, NULL)) < 0) {
		goto error;
	}

	if (leng != 1) {
		fprintf(stderr,"master query: wrong answer (leng)\n");
		goto error;
	}

	mfsstatus = ps_get8();
	if (mfsstatus == MFS_STATUS_OK) {
		// create ok
		printf("storage class %s has been created\n",scname);
	} else if (mfsstatus == MFS_ERROR_CLASSEXISTS) {
		if (overwrite) {
			ps_comminit();

			ps_put8(nleng);
			ps_putbytes((uint8_t*)scname,nleng);

			ps_put8(protocol);
			ps_put16(0xFFFF);	// change everything !!!
			serialize_sc_new(protocol,sc);
			if ((leng = ps_send_and_receive(CLTOMA_SCLASS_CHANGE, MATOCL_SCLASS_CHANGE, NULL))<0) {
				goto error;
			}

			if (leng==0) {
				fprintf(stderr,"master query: wrong answer (leng)\n");
				goto error;
			}
			if (leng==1) {
				mfsstatus = ps_get8();
				fprintf(stderr,"error overwritting storage class %s: %s\n",scname,mfsstrerr(mfsstatus));
				goto error;
			}
			if (ps_get8() != protocol) {
				fprintf(stderr,"master query: wrong answer (wrong data format)\n");
				goto error;
			}
			if (deserialize_sc_new(protocol, leng-1, sc) < 0) {
				fprintf(stderr,"master query: wrong answer (deserialize stoage class)\n");
				goto error;
			}
			printf("storage class %s has been overwritten\n",scname);
			// overwrite OK
		} else {
			printf("storage class %s already exists - ignoring\n",scname);
			// ignore - ok
		}
	} else {
		fprintf(stderr,"error importing storage class %s: %s\n",scname,mfsstrerr(mfsstatus));
		goto error;
	}
	return 0;
error:
	reset_master();
	return -1;
}

int check_string(const char **buff,const char *str) {
	int sl = strlen(str);
	if (strncmp(*buff,str,sl)==0) {
		*buff += sl;
		return 1;
	}
	return 0;
}

static inline int parse_label_expr_nowhite(const char **exprstr,parser_data *pd,char *exprbuff) {
	int i = 0;
	const char *p = *exprstr;
	while (i<65535 && *p && *p!=' ' && *p!='\t') {
		exprbuff[i++] = *p++;
	}
	exprbuff[i]='\0';
	*exprstr = p;
	return parse_label_expr(exprbuff,pd);
}

int import_sc(const char *mfsmp, FILE *fd,uint8_t overwrite) {
	char *linebuff;
	char *exprbuff;
	size_t lbsize;
	char *rp;
	const char *p;
	storage_class sc;
	char *scname;
	int perr;
	int status = -1;

	scname = NULL;
	exprbuff = NULL;
	linebuff = NULL;

	lbsize = 65536;
	linebuff = malloc(lbsize);
	if (linebuff==NULL) {
		fprintf(stderr,"can't allocate memory\n");
		goto error;
	}

	exprbuff = malloc(65536);
	if (exprbuff==NULL) {
		fprintf(stderr,"can't allocate memory\n");
		goto error;
	}

	while (getline(&linebuff,&lbsize,fd)!=-1) {
		rp = linebuff;
		while (*rp) {
			rp++;
		}
		while (rp>linebuff && (rp[-1]=='\r' || rp[-1]=='\n' || rp[-1]==' ' || rp[-1]=='\t')) {
			rp--;
			*rp = '\0';
		}
		p = linebuff;
		perr = 0;
		while (*p==' ' || *p=='\t') {
			p++;
		}
		if (check_string(&p,"name: ")) {
			if (scname!=NULL) {
				if (make_or_change_sc(mfsmp,scname,&sc,overwrite)<0) {
					goto error;
				}
			}
			free(scname);
			memset(&sc,0,sizeof(storage_class));
			scname = strdup(p);
		} else if (check_string(&p,"classid: ")) {
			// ignore
		} else if (check_string(&p,"description: ")) {
			while (*p>=32 && (sc.dleng+1)<MAXSCLASSDESCLENG) {
				sc.desc[sc.dleng] = *p;
				sc.dleng++;
				p++;
			}
			if (*p) {
				perr = 1;
			}
		} else if (check_string(&p,"priority: ")) {
			sc.priority = 0;
			while (*p>='0' && *p<='9') {
				sc.priority *= 10;
				sc.priority += (*p - '0');
				p++;
			}
			if (perr==0) {
				if (check_string(&p," ; export_group: ")) {
					sc.export_group = 0;
					while (*p>='0' && *p<='9') {
						sc.export_group *= 10;
						sc.export_group += (*p - '0');
						p++;
						if (sc.export_group>=EXPORT_GROUPS) {
							perr = 1;
						}
					}
					if (*p) {
						perr = 1;
					}
				} else {
					perr = 1;
				}
			}
		} else if (check_string(&p,"admin_only: ")) {
			if (check_string(&p,"YES")) {
				sc.admin_only = 1;
			} else if (check_string(&p,"NO")) {
				sc.admin_only = 0;
			} else {
				perr = 1;
			}
			if (perr==0) {
				if (check_string(&p," ; labels_mode: ")) {
					if (check_string(&p,"LOOSE")) {
						sc.labels_mode = LABELS_MODE_LOOSE;
					} else if (check_string(&p,"STRICT")) {
						sc.labels_mode = LABELS_MODE_STRICT;
					} else if (check_string(&p,"STD")) {
						sc.labels_mode = LABELS_MODE_STD;
					} else {
						perr = 1;
					}
				} else {
					perr = 1;
				}
			}
			if (perr==0) {
				if (check_string(&p," ; arch_mode: ")) {
					while (*p) {
						if (*p=='R') {
							sc.arch_mode |= SCLASS_ARCH_MODE_REVERSIBLE;
						} else if (*p=='A') {
							sc.arch_mode |= SCLASS_ARCH_MODE_ATIME;
						} else if (*p=='M') {
							sc.arch_mode |= SCLASS_ARCH_MODE_MTIME;
						} else if (*p=='C') {
							sc.arch_mode |= SCLASS_ARCH_MODE_CTIME;
						} else if (*p=='F') {
							sc.arch_mode |= SCLASS_ARCH_MODE_FAST;
						} else if (*p=='P') {
							sc.arch_mode |= SCLASS_ARCH_MODE_CHUNK;
						} else {
							perr = 1;
						}
						p++;
					}
				} else {
					perr = 1;
				}
			}
		} else if (check_string(&p,"create_labels: ")) {
			if (check_string(&p,"not defined")) {
				// just ignore
			} else {
				if (parse_label_expr_nowhite(&p,&(sc.create),exprbuff)<0) {
					perr = 1;
				}
			}
		} else if (check_string(&p,"keep_labels: ")) {
			if (parse_label_expr_nowhite(&p,&(sc.keep),exprbuff)<0) {
				perr = 1;
			}
		} else if (check_string(&p,"arch_labels: ")) {
			if (check_string(&p,"not defined")) {
				// just ignore
			} else {
				if (parse_label_expr_nowhite(&p,&(sc.arch),exprbuff)<0) {
					perr = 1;
				}
				if (perr==0) {
					if (check_string(&p," ; arch_delay: ")) {
						if (parse_time(&p,&(sc.arch_delay))<0) {
							perr = 1;
						}
					}
				}
				if (perr==0) {
					if (check_string(&p," ; arch_min_size: ")) {
						sc.arch_min_size = 0;
						while (*p>='0' && *p<='9') {
							sc.arch_min_size *= 10;
							sc.arch_min_size += (*p - '0');
							p++;
						}
					}
				}
			}
		} else if (check_string(&p,"trash_labels: ")) {
			if (check_string(&p,"not defined")) {
				// just ignore
			} else {
				if (parse_label_expr_nowhite(&p,&(sc.trash),exprbuff)<0) {
					perr = 1;
				}
				if (perr==0) {
					if (check_string(&p," ; min_trashretention: ")) {
						if (parse_time(&p,&(sc.min_trashretention))<0) {
							perr = 1;
						}
					}
				}
			}
		} else if (*p=='#' || *p==';' || *p=='\0') {
			// just ignore empty lines and lines starting with '#' or ';'
		} else {
			perr = 1;
		}
		if (perr) {
			fprintf(stderr,"error parsing line: %s\n",linebuff);
			goto error;
		}
	}
	if (scname!=NULL) {
		if (make_or_change_sc(mfsmp,scname,&sc,overwrite)<0) {
			goto error;
		}
	}
	status = 0;
error:
	if (scname!=NULL) {
		free(scname);
	}
	if (linebuff!=NULL) {
		free(linebuff);
	}
	if (exprbuff!=NULL) {
		free(exprbuff);
	}
	return status;
}

//----------------------------------------------------------------------

static const char *createmodifysctxt[] = {
	" -c - set description",
	" -p - set join priority (default is 0)",
	" -g - set export group (0..15, default is 0)",
	" -a - set admin only mode (0 - anybody can use this storage class, 1 - only admin can use this storage class) - by default it set to 0",
	" -m - set labels mode (options are: 'l'/'L' for 'loose', 's'/'S' for 'strict' and 'd'/'D' or not specified for 'default')",
	"    'default' mode: if there are overloaded servers, system will wait for them, but in case of no space available will use other servers (disregarding the labels).",
	"    'strict' mode: the system will wait for overloaded servers, but will return error (ENOSPC) when there is no space on servers with correct labels.",
	"    'loose' mode: the system will disregard the labels in both cases.",
	" -o - set arch mode ('a'/'A' - use atime, 'm'/'M' - use mtime, 'c'/'C' - use ctime, 'r'/'R' - reversible, 'f'/'F' - fast mode, 'p'/'P' - per chunk mode)",
	"    flags 'a', 'm', 'c' and 'r' can be defined together. When there are more than one time defined then maximum is used.",
	"    'r' means that after refreshing (a/m/c)time chunk can go back from 'archive' to 'keep', without it only transition from 'keep' to 'archive' is automatic.",
	"    'f' means that chunk is moved from 'keep' to 'archive' during regular maintenance loops (usually tens of minutes after creation/modification, '-d' option is disregarded).",
	"    'p' means that modification time of chunk is used instead of file's a/m/c time.",
	" -C - set labels used for creation chunks - when not specified then 'keep' labels are used",
	" -K - set labels used for keeping chunks - must be specified",
	" -A - set labels used for archiving chunks - when not specified then 'keep' labels are used",
	" -d - set delay used to switch labels from 'keep' to 'archive' - if not specified default value 24[h] will be used",
	" -s - set minimum length of file that can be allowed to be switched from 'keep' to 'archive'",
	" -T - set labels used for trash chunks - when not specified then 'archive' or 'keep' labels are used",
	" -t - set time used as 'minimum trash retention' required for chunk to apply trash settings",
	"",
	"To implicitly declare empty labels for options: C,A,T use one character labels definition: '-' or '~', especially useful with modify|change operation to set an empty definition.",
	"",
	"Options '-d' and '-t' can be defined as a number of hours (integer) or a time period in one of two possible formats:",
	"first format: #.#T where T is one of: h-hours, d-days or w-weeks; fractions of hours will be rounded to full hours",
	"second format: #w#d#h, any number of definitions can be ommited, but the remaining definitions must be in order (so #w#h is still a valid definition, but #d#w is not);",
	"ranges: h: 0 to 23, d: 0 to 6, w is unlimited and the first definition is also always unlimited (i.e. for #d#h d will be unlimited)",
	NULL
};

static const char *createsctxt[] = {
	"create new storage class",
	"",
	"usage: "_EXENAME_" [-?] [-M mount_point] -K keep_labels [-c description] [-p priority] [-g export_group] [-a admin_only] [-m labels_mode] [-o arch_mode] [-C create_labels] [-A archive_labels] [-d archive_delay] [-s archive_minimum_file_length] [-T trash_labels] [-t min_trashretention] sclass_name [sclass_name ...]",
	"",
	_QMARKDESC_,
	_MOUNTPOINTDESC_,
	NULL
};

static const char *modifysctxt[] = {
	"modify existing storage class",
	"",
	"usage: "_EXENAME_" [-?] [-M mount_point] [-c description] [-p priority] [-g export_group] [-a admin_only] [-m labels_mode] [-o arch_mode] [-C create_labels] [-K keep_labels] [-A archive_labels] [-d archive_delay] [-s archive_minimum_file_length] [-T trash_labels] [-t min_trashretention] sclass_name [sclass_name ...]",
	"",
	_QMARKDESC_,
	_MOUNTPOINTDESC_,
	NULL
};

static const char *deletesctxt[] = {
	"delete storage class",
	"",
	"usage: "_EXENAME_" [-?] [-M mount_point] sclass_name [sclass_name ...]",
	"",
	_QMARKDESC_,
	_MOUNTPOINTDESC_,
	NULL
};

static const char *clonesctxt[] = {
	"clone storage class",
	"",
	"usage: "_EXENAME_" [-?] [-M mount_point] src_sclass_name dst_sclass_name [dst_sclass_name ...]",
	"",
	_QMARKDESC_,
	_MOUNTPOINTDESC_,
	NULL
};

static const char *renamesctxt[] = {
	"rename storage class",
	"",
	"usage: "_EXENAME_" [-?] [-M mount_point] src_sclass_name dst_sclass_name",
	"",
	_QMARKDESC_,
	_MOUNTPOINTDESC_,
	NULL
};

static const char *listsctxt[] = {
	"list available storage classes",
	"",
	"usage: "_EXENAME_" [-?] [-M mount_point] [-li] [sclass_name_glob_pattern ...]",
	"",
	_QMARKDESC_,
	_MOUNTPOINTDESC_,
	" -l - lists storage classes with definitions (long format)",
	" -i - case insensitive storage class name matching",
	NULL
};

static const char *importsctxt[] = {
	"import storage classes from file",
	"",
	"usage: "_EXENAME_" [-?] [-M mount_point] [-r] [-n filename]",
	"",
	_QMARKDESC_,
	_MOUNTPOINTDESC_,
	" -r - replace (overwrite) existing storage classes (by default existing classes are skipped)",
	" -n - name of file with storage class definitions (format as in result of list with '-l' option)",
	NULL
};

void createscusage(void) {
	tcomm_print_help(createsctxt);
	tcomm_print_help(createmodifysctxt);
	exit(1);
}

void modifyscusage(void) {
	tcomm_print_help(modifysctxt);
	tcomm_print_help(createmodifysctxt);
	exit(1);
}

void deletescusage(void) {
	tcomm_print_help(deletesctxt);
	exit(1);
}

void clonescusage(void) {
	tcomm_print_help(clonesctxt);
	exit(1);
}

void renamescusage(void) {
	tcomm_print_help(renamesctxt);
	exit(1);
}

void listscusage(void) {
	tcomm_print_help(listsctxt);
	exit(1);
}

void importscusage(void) {
	tcomm_print_help(importsctxt);
	exit(1);
}

int change_modify_common_opts(int option,char *optarg_value,storage_class *sc,uint16_t *chgmask) {
	uint64_t v;
	char c;
	switch (option) {
		case 'A':
			if ((*chgmask) & SCLASS_CHG_ARCH_MASKS) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (parse_label_expr(optarg_value, &(sc->arch))<0) {
				return -1;
			}
			(*chgmask) |= SCLASS_CHG_ARCH_MASKS;
			break;
		case 'K':
			if ((*chgmask) & SCLASS_CHG_KEEP_MASKS) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (parse_label_expr(optarg_value, &(sc->keep))<0) {
				return -1;
			}
			(*chgmask) |= SCLASS_CHG_KEEP_MASKS;
			break;
		case 'C':
			if ((*chgmask) & SCLASS_CHG_CREATE_MASKS) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (parse_label_expr(optarg_value, &(sc->create))<0) {
				return -1;
			}
			(*chgmask) |= SCLASS_CHG_CREATE_MASKS;
			break;
		case 'T':
			if ((*chgmask) & SCLASS_CHG_TRASH_MASKS) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (parse_label_expr(optarg_value, &(sc->trash))<0) {
				return -1;
			}
			(*chgmask) |= SCLASS_CHG_TRASH_MASKS;
			break;
		case 'd':
			if ((*chgmask) & SCLASS_CHG_ARCH_DELAY) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (tparse_hours(optarg_value,&(sc->arch_delay))<0) {
				fprintf(stderr,"bad 'archive delay'\n");
				return -1;
			}
			(*chgmask) |= SCLASS_CHG_ARCH_DELAY;
			break;
		case 't':
			if ((*chgmask) & SCLASS_CHG_MIN_TRASHRETENTION) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (tparse_hours(optarg_value,&(sc->min_trashretention))<0) {
				fprintf(stderr,"bad 'minimum trash retention'\n");
				return -1;
			}
			(*chgmask) |= SCLASS_CHG_MIN_TRASHRETENTION;
			break;
		case 'm':
			if ((*chgmask) & SCLASS_CHG_LABELS_MODE) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (optarg_value[0]=='l' || optarg_value[0]=='L') {
				sc->labels_mode = LABELS_MODE_LOOSE;
			} else if (optarg_value[0]=='s' || optarg_value[0]=='S') {
				sc->labels_mode = LABELS_MODE_STRICT;
			} else if (optarg_value[0]=='d' || optarg_value[0]=='D') {
				sc->labels_mode = LABELS_MODE_STD;
			} else {
				fprintf(stderr,"unknown labels mode (option '%c')\n",option);
				return -1;
			}
			(*chgmask) |= SCLASS_CHG_LABELS_MODE;
			break;
		case 'c':
			if ((*chgmask) & SCLASS_CHG_DESCRIPTION) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (name_validate(optarg_value)<0) {
				fprintf(stderr,"bad characters in description string\n");
				return -1;
			}
			v = strlen(optarg_value);
			if (v>=MAXSCLASSDESCLENG) {
				fprintf(stderr,"bad description length\n");
				return -1;
			}
			sc->dleng = v;
			memcpy(sc->desc,optarg_value,v);
			(*chgmask) |= SCLASS_CHG_DESCRIPTION;
			break;
		case 'p':
			if ((*chgmask) & SCLASS_CHG_PRIORITY) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (my_get_number(optarg_value, &v,UINT32_MAX,0)<0) {
				fprintf(stderr,"bad priority\n");
				return -1;
			}
			sc->priority = v;
			(*chgmask) |= SCLASS_CHG_PRIORITY;
			break;
		case 'g':
			if ((*chgmask) & SCLASS_CHG_EXPORT_GROUP) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (my_get_number(optarg_value, &v,EXPORT_GROUPS-1,0)<0) {
				fprintf(stderr,"bad export group\n");
				return -1;
			}
			sc->export_group = v;
			(*chgmask) |= SCLASS_CHG_EXPORT_GROUP;
			break;
		case 'a':
			if ((*chgmask) & SCLASS_CHG_ADMIN_ONLY) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (optarg_value[0]=='0' || optarg_value[0]=='n' || optarg_value[0]=='N' || optarg_value[0]=='f' || optarg_value[0]=='F') {
				sc->admin_only = 0;
			} else if (optarg_value[0]=='1' || optarg_value[0]=='y' || optarg_value[0]=='Y' || optarg_value[0]=='t' || optarg_value[0]=='T') {
				sc->admin_only = 1;
			} else {
				fprintf(stderr,"unknown value for admin only flag (option '%c')\n",option);
				return -1;
			}
			(*chgmask) |= SCLASS_CHG_ADMIN_ONLY;
			break;
		case 's':
			if ((*chgmask) & SCLASS_CHG_ARCH_MIN_SIZE) {
				fprintf(stderr,"option '%c' defined twice\n",option);
				return -1;
			}
			if (my_get_number(optarg_value, &v,UINT64_MAX,1)<0) {
				fprintf(stderr,"bad min arch size\n");
				return -1;
			}
			sc->arch_min_size = v;
			(*chgmask) |= SCLASS_CHG_ARCH_MIN_SIZE;
			break;
		case 'o':
			if (((*chgmask) & SCLASS_CHG_ARCH_MODE)==0) {
				sc->arch_mode = 0;
			}
			while ((c=*optarg_value++)) {
				switch ((c|0x20)) {
					case 'a':
						sc->arch_mode |= SCLASS_ARCH_MODE_ATIME;
						break;
					case 'c':
						sc->arch_mode |= SCLASS_ARCH_MODE_CTIME;
						break;
					case 'm':
						sc->arch_mode |= SCLASS_ARCH_MODE_MTIME;
						break;
					case 'r':
						sc->arch_mode |= SCLASS_ARCH_MODE_REVERSIBLE;
						break;
					case 'f':
						sc->arch_mode |= SCLASS_ARCH_MODE_FAST;
						break;
					case 'p':
						sc->arch_mode |= SCLASS_ARCH_MODE_CHUNK;
						break;
					default:
						return -1;
				}
			}
			(*chgmask) |= SCLASS_CHG_ARCH_MODE;
			break;
	}
	return 0;
}

int check_arch_mode(uint8_t arch_mode) {
	if (arch_mode==0) {
		fprintf(stderr,"Empty arch mode\n");
		return -1;
	}
	if ((arch_mode & SCLASS_ARCH_MODE_CHUNK) && (arch_mode & ~SCLASS_ARCH_MODE_CHUNK)) {
		fprintf(stderr,"Chunk arch mode ('P') is mutually exclusive with all other arch modes\n");
		return -1;
	}
	if ((arch_mode & SCLASS_ARCH_MODE_FAST) && (arch_mode & ~SCLASS_ARCH_MODE_FAST)) {
		fprintf(stderr,"Fast arch mode ('F') is mutually exclusive with all other arch modes\n");
		return -1;
	}
	return 0;
}

int createscexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	storage_class sc;
	uint16_t chgmask;
	char *mountpoint;

	mountpoint = NULL;
	memset(&sc,0,sizeof(storage_class));
	sc.labels_mode = LABELS_MODE_STD;
	sc.arch_delay = 24;
	sc.arch_mode = SCLASS_ARCH_MODE_CTIME;
	chgmask = 0;

	while ((ch=getopt(argc,argv,"?M:A:K:C:T:d:t:m:c:p:g:a:s:o:"))!=-1) {
		switch (ch) {
			case 'M':
				if (mountpoint!=NULL) {
					free(mountpoint);
				}
				mountpoint = strdup(optarg);
				break;
			case 'A':
			case 'K':
			case 'C':
			case 'T':
			case 'd':
			case 't':
			case 'm':
			case 'c':
			case 'p':
			case 'g':
			case 'a':
			case 's':
			case 'o':
				if (change_modify_common_opts(ch,optarg,&sc,&chgmask)<0) {
					createscusage();
				}
				break;
			default:
				createscusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (check_arch_mode(sc.arch_mode)<0) {
		createscusage();
		return 1;
	}
	if ((chgmask & SCLASS_CHG_KEEP_MASKS)==0) {
		fprintf(stderr,"option '-K' not specified\n");
		createscusage();
		return 1;
	}
	if (sc.keep.ec_data_chksum_parts) {
		fprintf(stderr,"option '-K' with EC defined - EC can be defined only in archive ('-A') and trash ('-T') modes\n");
		createscusage();
		return 1;
	}
	if (sc.create.ec_data_chksum_parts) {
		fprintf(stderr,"option '-C' with EC defined - EC can be defined only in archive ('-A') and trash ('-T') modes\n");
		createscusage();
		return 1;
	}

	if (argc==0) {
		createscusage();
		return 1;
	}

	if (names_validate(argc,argv)<0) {
		fprintf(stderr,"bad characters in class name\n");
		createscusage();
		return 1;
	}

	while (argc>0) {
		if (make_sc(mountpoint,*argv,&sc)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int modifyscexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	storage_class sc;
	uint16_t chgmask;
	char *mountpoint;

	mountpoint = NULL;
	memset(&sc,0,sizeof(storage_class));
	sc.labels_mode = LABELS_MODE_STD;
	sc.arch_delay = 24;
	sc.arch_mode = SCLASS_ARCH_MODE_CTIME;
	chgmask = 0;

	while ((ch=getopt(argc,argv,"?M:A:K:C:T:d:t:m:c:p:g:a:s:o:"))!=-1) {
		switch (ch) {
			case 'M':
				if (mountpoint!=NULL) {
					free(mountpoint);
				}
				mountpoint = strdup(optarg);
				break;
			case 'A':
			case 'K':
			case 'C':
			case 'T':
			case 'd':
			case 't':
			case 'm':
			case 'c':
			case 'p':
			case 'g':
			case 'a':
			case 's':
			case 'o':
				if (change_modify_common_opts(ch,optarg,&sc,&chgmask)<0) {
					modifyscusage();
				}
				break;
			default:
				modifyscusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (check_arch_mode(sc.arch_mode)<0) {
		modifyscusage();
		return 1;
	}
	if (sc.keep.ec_data_chksum_parts) {
		fprintf(stderr,"option '-K' with EC defined - EC can be defined only in archive ('-A') and trash ('-T') modes\n");
		modifyscusage();
		return 1;
	}
	if (sc.create.ec_data_chksum_parts) {
		fprintf(stderr,"option '-C' with EC defined - EC can be defined only in archive ('-A') and trash ('-T') modes\n");
		modifyscusage();
		return 1;
	}


	if (argc==0) {
		modifyscusage();
		return 1;
	}

	if (names_validate(argc,argv)<0) {
		fprintf(stderr,"bad characters in class name\n");
		modifyscusage();
		return 1;
	}

	while (argc>0) {
		if (change_sc(mountpoint,*argv,chgmask,&sc)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int deletescexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	char *mountpoint;

	mountpoint = NULL;

	while ((ch=getopt(argc,argv,"?M:"))!=-1) {
		switch (ch) {
			case 'M':
				if (mountpoint!=NULL) {
					free(mountpoint);
				}
				mountpoint = strdup(optarg);
				break;
			default:
				deletescusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc==0) {
		deletescusage();
		return 1;
	}

	if (names_validate(argc,argv)<0) {
		fprintf(stderr,"bad characters in class name\n");
		deletescusage();
		return 1;
	}

	while (argc>0) {
		if (remove_sc(mountpoint,*argv)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int clonescexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	char *mountpoint;
	char *src_storage_class_name;

	mountpoint = NULL;

	while ((ch=getopt(argc,argv,"?M:"))!=-1) {
		switch (ch) {
			case 'M':
				if (mountpoint!=NULL) {
					free(mountpoint);
				}
				mountpoint = strdup(optarg);
				break;
			default:
				clonescusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=1) {
		clonescusage();
		return 1;
	}

	if (names_validate(argc,argv)<0) {
		fprintf(stderr,"bad characters in class name\n");
		clonescusage();
		return 1;
	}

	src_storage_class_name = strdup(*argv);

	argc--;
	argv++;

	while (argc>0) {
		if (clone_sc(mountpoint,src_storage_class_name,*argv)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int renamescexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	char *mountpoint;
	char *src_storage_class_name;

	mountpoint = NULL;

	while ((ch=getopt(argc,argv,"?M:"))!=-1) {
		switch (ch) {
			case 'M':
				if (mountpoint!=NULL) {
					free(mountpoint);
				}
				mountpoint = strdup(optarg);
				break;
			default:
				renamescusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<=1) {
		renamescusage();
		return 1;
	}

	if (names_validate(argc,argv)<0) {
		fprintf(stderr,"bad characters in class name\n");
		renamescusage();
		return 1;
	}

	src_storage_class_name = strdup(*argv);

	argc--;
	argv++;

	while (argc>0) {
		if (move_sc(mountpoint,src_storage_class_name,*argv)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int listscexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	char *mountpoint;
	uint8_t insens;
	uint8_t longmode;

	mountpoint = NULL;
	insens = 0;
	longmode = 0;

	while ((ch=getopt(argc,argv,"?M:il"))!=-1) {
		switch (ch) {
			case 'M':
				if (mountpoint!=NULL) {
					free(mountpoint);
				}
				mountpoint = strdup(optarg);
				break;
			case 'i':
				insens = 1;
				break;
			case 'l':
				longmode = 1;
				break;
			default:
				listscusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (list_sc(mountpoint,longmode,insens,argc,argv)<0) {
		status = 1;
	}

	return status;
}

int importscexe(int argc,char *argv[]) {
	int ch;
	int status = 0;
	char *mountpoint;
	char *filename;
	FILE *fd;
	uint8_t overwrite;

	mountpoint = NULL;
	filename = NULL;
	overwrite = 0;

	while ((ch=getopt(argc,argv,"?M:rn:"))!=-1) {
		switch (ch) {
			case 'M':
				if (mountpoint!=NULL) {
					free(mountpoint);
				}
				mountpoint = strdup(optarg);
				break;
			case 'r':
				overwrite = 1;
				break;
			case 'n':
				if (filename!=NULL) {
					free(filename);
				}
				filename = strdup(optarg);
				break;
			default:
				importscusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc>0) {
		importscusage();
		return 1;
	}

	if (filename==NULL || strcmp(filename,"-")==0) {
		fd = stdin;
	} else {
		fd = fopen(filename,"r");
		if (fd==NULL) {
			fprintf(stderr,"can't open file: %s\n",filename);
			return 1;
		}
	}

	if (import_sc(mountpoint,fd,overwrite)<0) {
		status = 1;
	}

	if (fd!=stdin) {
		fclose(fd);
	}

	return status;
}

static command commandlist[] = {
	{"create | make", createscexe},
	{"modify | change", modifyscexe},
	{"delete | remove", deletescexe},
	{"clone | copy | duplicate", clonescexe},
	{"rename | move", renamescexe},
	{"list", listscexe},
	{"import", importscexe},
	{NULL,NULL}
};

int main(int argc,char *argv[]) {
	return tcomm_find_and_execute(argc,argv,commandlist);
}
