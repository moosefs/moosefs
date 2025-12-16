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

#ifndef _MIME_H_
#define _MIME_H_

typedef struct _mimetype {
	uint32_t extl;
	char *ext;
	char *mime;
} mimetype;

#define MIMEDEF(ext,mime) {sizeof(ext)-1,ext,mime}

static mimetype mimes[] = {
	MIMEDEF("txt","text/plain"),
	MIMEDEF("html","text/html; charset=utf-8"),
	MIMEDEF("css","text/css"),
	MIMEDEF("js","text/javascript"),
	MIMEDEF("ico","image/vnd.microsoft.icon"),
	MIMEDEF("gif","image/gif"),
	MIMEDEF("jpg","image/jpeg"),
	MIMEDEF("jpeg","image/jpeg"),
	MIMEDEF("png","image/png"),
	MIMEDEF("tiff","image/tiff"),
	MIMEDEF("tif","image/tiff"),
	MIMEDEF("bmp","image/bmp"),
	MIMEDEF("zip","application/zip"),
	MIMEDEF("xml","text/xml"),
	MIMEDEF("svg","image/svg+xml"),
	MIMEDEF("pdf","application/pdf"),
	MIMEDEF("ttf","application/octet-stream"),
	{0,NULL,NULL}
};

static const char* mime_find(const char *fname) {
	int el,fl;
	int i;

	fl = strlen(fname);

	for (i=0 ; (el=mimes[i].extl)>0 ; i++) {
		if (fl>el && fname[fl-el-1]=='.' && memcmp(fname+fl-el,mimes[i].ext,el)==0) {
			return mimes[i].mime;
		}
	}
	return NULL;
}

#endif
