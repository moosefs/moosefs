/*
 * Copyright (C) 2015 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

if (typeof acid_ready == "undefined") {
	acid_ready = {
		ready : 0,
		functions : new Array(),
		docready : function() {
			if (acid_ready.ready==0) {
				acid_ready.ready = 1;
				var i;
				for (i=0 ; i<acid_ready.functions.length ; i++) {
					acid_ready.functions[i]();
				}
			}
		},
		domloaded : function(e) {
			acid_ready.docready();
		},
		loaded : function(e) {
			acid_ready.docready();
		},
		readystate : function(e) {
			if (document.readyState=="complete" || document.readyState=="interactive") {
				acid_ready.docready();
			}
		},
		readytest : function() {
			if (acid_ready.ready==0) {
				if (typeof document.readyState!="undefined") {
					if (document.readyState=="complete" || document.readyState=="interactive") {
						acid_ready.docready();
					} else {
						setTimeout("acid_ready.readytest()",100);
					}
					return;
				}
				if (typeof document.documentElement!="undefined" && typeof document.documentElement.doScroll!="undefined") {
					try {
						document.documentElement.doScroll('left');
					} catch (e) {
						setTimeout("acid_ready.readytest()",100);
						return;
					}
				}
				acid_ready.docready();
			}
		},
		register : function(fn) {
			if (acid_ready.ready) {
				fn();
			} else {
				var l = acid_ready.functions.length;
				acid_ready.functions[l]=fn;
			}
		},
		init : function() {
			if (window.addEventListener) {
				document.addEventListener("DOMContentLoaded",acid_ready.domloaded,false);
				document.addEventListener("readystatechange",acid_ready.readystate,false);
				window.addEventListener("load",acid_ready.loaded,false);
			} else if (window.attachEvent) {
				document.attachEvent("onDOMContentLoaded",acid_ready.domloaded);
				document.attachEvent("onreadystatechange",acid_ready.readystate);
				window.attachEvent("onload",acid_ready.loaded);
			}
			setTimeout("acid_ready.readytest()",100);
		}
	}
	acid_ready.init();
}

if (typeof acid_tab == "undefined") {
	acid_tab = {
		init: function() {
			var tabs,i;
			if (!document.createElement || !document.getElementsByTagName) {
				return;
			}
			tabs = document.getElementsByTagName('table');
			if (tabs) {
				for (i=0 ; i<tabs.length ; i++) {
					if (tabs[i].className.search(/\bacid_tab\b/) != -1) {
						acid_tab.preparetab(tabs[i]);
					}
				}
			}
		},

		preparetab: function(table) {
			var i,j,k,x,h,m,s,p,c,z,r;
			// find thead using 'TH' node names
			if (table.getElementsByTagName('thead').length == 0) {
				var thead = document.createElement('thead');
				while (table.rows.length>0 && table.rows[0].cells[0].nodeName=="TH") {
					thead.appendChild(table.rows[0]);
				}
				table.insertBefore(thead,table.firstChild);
			}
			// create tHead if necessary
			if (typeof table.tHead == "undefined") {
				table.tHead = table.getElementsByTagName('thead')[0];
			}
			// backup and remove extra bodies
			table.acid_tab_tbodiesbackup = new Array();
			for (i=0 ; i<table.tBodies.length ; i++) {
				table.acid_tab_tbodiesbackup[i] = table.tBodies[i];
			}
			while (table.tBodies.length>1) {
				table.removeChild(table.tBodies[1]);
			}
			// no body? - exit
			if (table.tBodies.length==0) {
				return;
			}
			// check settings
			m = table.className.match(/\bacid_tab_zebra_([a-zA-Z0-9]+)_([a-zA-Z0-9]+)\b/);
			if (m) {
				table.acid_tab_zebra = new Array();
				table.acid_tab_zebra[0] = m[1];
				table.acid_tab_zebra[1] = m[2];
			}
			if (table.className.search(/\bacid_tab_noindicator\b/)!=-1) {
				table.acid_tab_indicator = 0;
			} else {
				table.acid_tab_indicator = 1;
			}
			m = table.className.match(/\bacid_tab_storageid_([a-zA-Z0-9]+)\b/);
			if (m) {
				table.acid_tab_storageid = m[1];
			} else {
				table.acid_tab_storageid = "";
			}
			z = -1;
			r = 0;
			// scan storage
			if (table.acid_tab_storageid!="" && typeof sessionStorage != "undefined") {
				p = "switchdisplay_"+table.acid_tab_storageid+"_";
				for (i=0 ; i<sessionStorage.length ; i++) {
					k = sessionStorage.key(i);
					if (k.slice(0,p.length)==p) {
						j = parseInt(sessionStorage.getItem(k));
						acid_tab.switchdisplay(table,k.slice(p.length),j);
					}
				}
				z = sessionStorage.getItem('switchbody_'+table.acid_tab_storageid);
				if (z) {
					z = parseInt(z);
					acid_tab.switchtbody(table,z);
				}
				z = sessionStorage.getItem('sortedby_'+table.acid_tab_storageid);
				if (z) {
					m = z.match(/\b([0-9]+)_(F|R)\b/);
					if (m) {
						z = parseInt(m[1]);
						r = (m[2]=='R')?1:0;
					}
				}
			}
			table.acid_tab_sortedby = null;
			table.acid_tab_reversed = 0;
			// find one-column cells in header and attach onlick event to them
			k = 0;
			s = new Array();
			for (i=0 ; i<table.tHead.rows.length ; i++) {
				var row = table.tHead.rows[i];
				p = 0;
				for (j=0 ; j<row.cells.length ; j++) {
					var cell = row.cells[j];
					if (typeof cell.colSpan == "undefined") {
						cell.colSpan = 1;
					}
					if (typeof cell.rowSpan == "undefined") {
						cell.rowSpan = 1;
					}
					// skip rowspans
					while (p<s.length && s[p]>i) {
						p++;
					}
					// set rowspans
					for (c=0 ; c<cell.colSpan ; c++) {
						s[p+c]=(i+cell.rowSpan);
					}
					// ignore multi-column cells
					if (cell.colSpan==1) {
						if (cell.className.search(/\bacid_tab_enumerate\b/)!=-1) {
							table.acid_tab_enumerate = p;
						} else if (cell.className.search(/\bacid_tab_skip\b/)==-1) {
							if (cell.className.search(/\bacid_tab_numeric\b/)!=-1) {
								cell.acid_tab_smode=1;
							} else if (cell.className.search(/\bacid_tab_alpha\b/)!=-1) {
								cell.acid_tab_smode=2;
							} else {
								cell.acid_tab_smode=0;
							}
							cell.acid_tab_colid = p;
							cell.acid_tab = table;
							if (cell.className.search(/\bacid_tab_nounderline\b/)==-1) {
								cell.style.textDecoration="underline";
							}
							if (cell.className.search(/\bacid_tab_nocursor\b/)==-1) {
								cell.style.cursor="pointer";
							}
							m = cell.className.match(/\bacid_tab_level_([0-9]+)\b/);
							if (m) {
								cell.acid_tab_level = m[1];
							} else {
								cell.acid_tab_level = 0;
							}
							if (cell.addEventListener) {
								cell.addEventListener("click",acid_tab.clickhandler,false);
							} else if (cell.attachEvent) {
								cell.attachEvent("onclick",acid_tab.clickhandler);
							}
							cell.acid_tab_myid = k;
							if (z==k) {
								table.acid_tab_sortedby = cell;
								table.acid_tab_reversed = r;
							}
							k++;
						}
					}
					// set pos
					p+=cell.colSpan;
				}
			}
			if (table.acid_tab_sortedby!=null && table.acid_tab_indicator) {
				table.acid_tab_indicator = document.createElement('span');
				table.acid_tab_indicator.innerHTML = (table.acid_tab_reversed)?'&nbsp;&#x25B2;':'&nbsp;&#x25BC;';
				table.acid_tab_sortedby.appendChild(table.acid_tab_indicator);
			}
			acid_tab.resort(table);
		},

		switchtbody: function(table,n) {
			if (n>=0 && n<table.acid_tab_tbodiesbackup.length) {
				table.removeChild(table.tBodies[0]);
				table.appendChild(table.acid_tab_tbodiesbackup[n]);
				if (table.acid_tab_storageid!="" && typeof sessionStorage != "undefined") {
					sessionStorage.setItem("switchbody_"+table.acid_tab_storageid,n);
				}
			}
			acid_tab.resort(table);
		},

		resort: function(table) {
			if (table.acid_tab_sortedby != null) {
				acid_tab.sorter(table.acid_tab_sortedby,1);
			} else {
				acid_tab.aftersort(table,table.tBodies[0]);
			}
		},

		getcelltext: function(cell,level) {
			var ret,i,visible;
			if (!cell) {
				return "";
			}
			if (level==0) {
				visible = cell.offsetWidth > 0 || cell.offsetHeight > 0;
				if (!visible) {
					return "";
				}
				if (typeof cell.textContent != "undefined") {
					return cell.textContent.replace(/^\s+|\s+$/g,''); // strip white spaces
				} else if (typeof cell.innerText != "undefined") {
					return cell.innerText.replace(/^\s+|\s+$/g,'');
				} else if (typeof cell.text != "undefined") {
					return cell.text.replace(/^\s+|\s+$/g,'');
				}
			}
			ret = '';
			for (i=0 ; i<cell.childNodes.length ; i++) {
				ret += acid_tab.getcelltext(cell.childNodes[i],(level>0)?level-1:0);
			}
			return ret;
		},

		sorter: function(hcell,newbody) {
			var table = hcell.acid_tab;
			var tbody = table.tBodies[0];
			var newrows = new Array();
			var smode = 0;
			var level = 0;
			var i;
			if (newbody==0) {
				if (table.acid_tab_sortedby != null) {
					if (table.acid_tab_indicator) {
						table.acid_tab_sortedby.removeChild(table.acid_tab_indicator);
					}
				}
				if (table.acid_tab_sortedby == hcell) {
					acid_tab.reverse(tbody);
					acid_tab.aftersort(table,tbody);
					table.acid_tab_reversed = 1-table.acid_tab_reversed;
					if (table.acid_tab_indicator) {
						table.acid_tab_indicator = document.createElement('span');
						table.acid_tab_indicator.innerHTML = (table.acid_tab_reversed)?'&nbsp;&#x25B2;':'&nbsp;&#x25BC;';
						hcell.appendChild(table.acid_tab_indicator);
					}
					acid_tab.remember(table);
					return;
				}
			}
			smode = hcell.acid_tab_smode;
			level = hcell.acid_tab_level;
			for (i=0 ; smode==0 && i<tbody.rows.length ; i++) {
				var cell = tbody.rows[i].cells[hcell.acid_tab_colid];
				var text = acid_tab.getcelltext(cell,level);
				if (text!='') {
					if (text.match(/^\s*(\+|-)?((\d+(\.\d+)?)|(\.\d+))(\s|%|$)/)) { // looks like a number
						smode = 1;
					}
				}
			}
			if (smode==0) { // number not found - switch to alpha
				smode = 2;
			}
			for (i=0 ; i<tbody.rows.length ; i++) {
				var cell = tbody.rows[i].cells[hcell.acid_tab_colid];
				var text = acid_tab.getcelltext(cell,level);
				if (smode==1) {
					text = parseFloat(text);
					if (isNaN(text)) {
						text = 0;
					}
				}
				newrows[newrows.length] = [text,tbody.rows[i]];
			}
			if (smode==1) {
				newrows.sort(function(a,b){return a[0]-b[0];});
			} else {
				newrows.sort(function(a,b){ return (a[0]==b[0])?0:(a[0]<b[0])?-1:1; });
			}
			if (newbody==0) {
				table.acid_tab_reversed = 0;
			}
			if (table.acid_tab_reversed) {
				for (i=newrows.length-1 ; i>=0 ; i--) {
					tbody.appendChild(newrows[i][1]);
				}
			} else {
				for (i=0 ; i<newrows.length ; i++) {
					tbody.appendChild(newrows[i][1]);
				}
			}
			delete newrows;
			acid_tab.aftersort(table,tbody);
			if (newbody==0) {
				table.acid_tab_sortedby = hcell;
				if (table.acid_tab_indicator) {
					table.acid_tab_indicator = document.createElement('span');
					table.acid_tab_indicator.innerHTML = (table.acid_tab_reversed)?'&nbsp;&#x25B2;':'&nbsp;&#x25BC;';
					hcell.appendChild(table.acid_tab_indicator);
				}
				acid_tab.remember(table);
			}
			return;
		},

		remember: function(table) {
			if (table.acid_tab_storageid!="" && typeof sessionStorage != "undefined") {
				var i;
				i = ""+table.acid_tab_sortedby.acid_tab_myid+"_"+(table.acid_tab_reversed?'R':'F');
				sessionStorage.setItem("sortedby_"+table.acid_tab_storageid,i);
			}
		},

		clickhandler: function(ev) {
			acid_tab.sorter(this,0);
		},

		reverse: function(tbody) {
			var newrows = new Array();
			var i;
			for (i=0 ; i<tbody.rows.length ; i++) {
				newrows[newrows.length] = tbody.rows[i];
			}
			for (i=newrows.length-1 ; i>=0 ; i--) {
				tbody.appendChild(newrows[i]);
			}
			delete newrows;
		},

		aftersort: function(table,tbody) {
			if (typeof table.acid_tab_enumerate != "undefined") {
				acid_tab.enumerate(tbody,table.acid_tab_enumerate);
			}
			if (typeof table.acid_tab_zebra != "undefined") {
				acid_tab.dozebra(tbody,table.acid_tab_zebra);
			}
		},

		dozebra: function(tbody,zebratab) {
			var i;
			for (i=0 ; i<tbody.rows.length ; i++) {
				tbody.rows[i].className=zebratab[i%zebratab.length];
			}
		},

		enumerate: function(tbody,colid) {
			var i;
			for (i=0 ; i<tbody.rows.length ; i++) {
				tbody.rows[i].cells[colid].innerHTML=i+1;
			}
		},

		changecss: function(myclass,element,value) {
			var i,j;
			var found;
			found = 0;
			for (i=0 ; i<document.styleSheets.length ; i++) {
				for (j=0 ; j<document.styleSheets[i].cssRules.length ; j++) {
					if (document.styleSheets[i].cssRules[j].selectorText == myclass) {
						document.styleSheets[i].cssRules[j].style[element] = value;
						found = 1;
					}
				}
			}
			return found;
		},

		switchdisplay: function(table,sname,no) {
			var i,d,f;
			i = 0;
			do {
				d = (i==no)?'inline':'none';
				f = acid_tab.changecss('span.'+sname+i,'display',d);
				i++;
			} while (f);
			if (typeof table.nodeName == "undefined") {
				if (typeof table.toString != "undefined") {
					table = table.toString();
				}
				if (typeof table == "string") {
					table = document.getElementById(table);
				}
			}
			while (table.nodeName!="TABLE" && table.parentNode) {
				table = table.parentNode;
			}
			if (table.nodeName=="TABLE") {
				acid_tab.resort(table);
				if (table.acid_tab_storageid!="" && typeof sessionStorage != "undefined") {
					sessionStorage.setItem("switchdisplay_"+table.acid_tab_storageid+"_"+sname,no);
				}
			}
		}
	}
	acid_ready.register(acid_tab.init);
}
