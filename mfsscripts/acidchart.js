/*
Copyright (C) 2025 Jakub Kruszona-Zawadzki, Saglabs SA

This file is part of MooseFS.

MooseFS is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, version 2 (only).

MooseFS is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with MooseFS; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

if (typeof acid_canvas_string == "undefined") {
	acid_canvas_string = {
		font: null,
		convarr: [],
		init: function() {
			var i;
			var charstring = "0123456789.:kMGTPEZYmu% "

			this.font = new Image();

			this.font.src = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAH0AAAAJAQMAAAAl/bGxAAAABGdBTUEAALGPC/xhBQAAAAZQTFRFAAAAAAAApWe5zwAAAAF0Uk5TAEDm2GYAAAABYktHRACIBR1IAAAAhUlEQVQI12MolP1Yc7+4gcX1/vf/AswKPxi6lZQdGXscWPJUepgFmB06GHqZXKuYehxcrrH0sKz3AAp0srxkP1R8gNUt5TuH14kGoACHfJtKh4Obm0qLgNdJBqCAoKCbSkcHK1BAwetkQgdD+b+HlkeKOViOHmn/5/Ut4QcDCnBgYMAQAADFWizkl7UTcgAAAABJRU5ErkJggg==";

			for (i=0 ; i<128 ; i++) {
				this.convarr[i] = 24;
			}
			for (i=0 ; i<charstring.length ; i++) {
				this.convarr[charstring.charCodeAt(i)] = i;
			}
			this.convarr[",".charCodeAt(0)]=10;
		},
		draw: function(ctx,x,y,str) {
			var i,c;
			for (i=0 ; i<str.length ; i++) {
				c = str.charCodeAt(i);
				if (c>=0 && c<128) {
					pos = this.convarr[c];
				} else {
					pos = 24;
				}
				ctx.drawImage(this.font,pos*5,0,5,9,x+6*i,y,5,9);
			}
		}
	}
	acid_canvas_string.init();
	CanvasRenderingContext2D.prototype.drawStringAt = function(x,y,str) {
		acid_canvas_string.draw(this,x,y,str);
	};
}

function AcidChart(idprefix,chartgroup) {
	var idparent = idprefix + "p";
	var idleft = idprefix + "l";
	var idcenter = idprefix + "c";
	var idright = idprefix + "r";
	this.eleml = document.getElementById(idleft);
	this.elemc = document.getElementById(idcenter);
	this.elemr = document.getElementById(idright);
	this.elemp = document.getElementById(idparent);
	var chartheight = this.elemc.clientHeight - 20;
	var chartwidth = this.elemc.clientWidth * 2;
	chartheight = Math.floor(chartheight/20)*20;
	if (chartwidth<4095) {
		chartwidth = 4095;
	}
	this.canvasl = document.createElement("canvas");
	this.canvasc = document.createElement("canvas");
	this.canvasr = document.createElement("canvas");
	this.canvasl.width = 43;
	this.canvasr.width = 7;
	this.canvasc.width = chartwidth;
	this.canvasl.height = chartheight + 20;
	this.canvasr.height = chartheight + 20;
	this.canvasc.height = chartheight + 20;
	this.chartdata = null;
	this.ctxl = this.canvasl.getContext("2d");
	this.ctxr = this.canvasr.getContext("2d");
	this.ctxc = this.canvasc.getContext("2d");
	this.chartx = chartwidth;
	this.charty = chartheight;
	this.chartoffx = 43;
	this.chartoffy = 6;
	this.series = 0;
	this.values = 0;
	this.grdnull = null;
	this.grd1 = null;
	this.grd2 = null;
	this.grd3 = null;
	this.hoff = 0;
	this.max = 0;
	this.dmax = 0;
	this.step = 0;
	this.mode = 0;
	this.range = 0;
	this.minval = 0;
	this.suffix = "";
	this.position = 0;
	this.common_scale = 0;
	this.chartxdata = chartwidth; // 0
	this.maxposition = this.chartxdata - this.elemc.clientWidth;
	this.mousex = 0;
	this.heightbase = chartheight;
	this.heightdiff = this.elemp.clientHeight - chartheight;
	this.hmode = 0;
	this.loading = 0;
	this.elemc.acid_chart = this;
	this.eleml.acid_chart = this;
	if (document.addEventListener) {
		this.elemc.addEventListener('mousedown',this.mouse_down_handler,false);
		this.elemc.addEventListener('dblclick',this.click_handler,false);
		this.eleml.addEventListener('click',this.click_handler,false);
	} else if (document.attachEvent) {
		this.elemc.attachEvent('onmousedown',this.mouse_down_handler);
		this.elemc.attachEvent('ondblclick',this.click_handler);
		this.eleml.attachEvent('onclick',this.click_handler);
	}
	this.chartgroup = chartgroup;
	if (typeof window.acid_chart_groups == "undefined") {
		window.acid_chart_groups = {};
	}
	if (typeof window.acid_chart_groups[chartgroup] == "undefined") {
		window.acid_chart_groups[chartgroup] = []
	}
	window.acid_chart_groups[chartgroup].push(this);
	if (typeof window.acid_charts == "undefined") {
		window.acid_charts = [];
		if (window.addEventListener) {
			window.addEventListener('resize',this.window_resize_handler,false);
		} else if (window.attachEvent) {
			window.attachEvent('onresize',this.window_resize_handler);
		}
	}
	window.acid_charts.push(this);
}
AcidChart.prototype.format_hour = function(h) {
	if (h<10) {
		return "0"+h.toString()+":00";
	} else {
		return h.toString()+":00";
	}
}
AcidChart.prototype.format_day = function(m,d) {
	var ms,ds;
	if (m<10) {
		ms = "0"+m.toString();
	} else {
		ms = m.toString();
	}
	if (d<10) {
		ds = "0"+d.toString();
	} else {
		ds = d.toString();
	}
	return ms+"."+ds;
}
AcidChart.prototype.format_month = function(m) {
	if (m<10) {
		return "0"+m.toString();
	} else {
		return m.toString();
	}
}
// mode=0 : #.##
// mode=1 : ##.#
// mode=2 :  ###
AcidChart.prototype.format_number = function(n,mode) {
	var s = "";
	if (mode==0) {
		rn = Math.round(n*100);
		if (rn>999 || rn<0) {
			return ":.::";
		}
		return (Math.floor(rn/100)%10).toString() + "." + (Math.floor(rn/10)%10).toString() + (rn%10).toString();
	} else if (mode==1) {
		rn = Math.round(n*10);
		if (rn>999 || rn<0) {
			return "::.:";
		}
		if (rn<100) {
			return " "+ (Math.floor(rn/10)%10).toString() + "." + (rn%10).toString();
		} else {
			return (Math.floor(rn/10)%100).toString() + "." + (rn%10).toString();
		}
	} else {
		rn = Math.round(n);
		if (rn>999 || rn<0) {
			return " :::";
		}
		if (rn<10) {
			return "   "+Math.floor(rn%10).toString();
		} else if (rn<100) {
			return "  "+Math.floor(rn%100).toString();
		} else {
			return " "+Math.floor(rn%1000).toString();
		}
	}
}
AcidChart.prototype.month_days = function(y,m) {
	var mdays = [31,28,31,30,31,30,31,31,30,31,30,31];
	if (m!=1) {
		return mdays[m];
	} else {
		if (y%4) {
			return 28;
		}
		if (y%100) {
			return 29;
		}
		if (y%400) {
			return 28;
		}
		return 29;
	}
}
AcidChart.prototype.create_gradients = function() {
	this.grdnull = this.ctxc.createLinearGradient(0, this.chartoffy, 0, this.chartoffy+this.charty);
	this.grd1 = this.ctxc.createLinearGradient(0, this.chartoffy, 0, this.chartoffy+this.charty);
	this.grd2 = this.ctxc.createLinearGradient(0, this.chartoffy, 0, this.chartoffy+this.charty);
	this.grd3 = this.ctxc.createLinearGradient(0, this.chartoffy, 0, this.chartoffy+this.charty);
	this.grdnull.addColorStop(0, "#F0F0F0"); //no-data background
	this.grdnull.addColorStop(1, "#A0A0A0");
	// magenta gradients
//	if (this.series==1) {
//		this.grd1.addColorStop(0, "#00245E");
//		this.grd1.addColorStop(0.5, "#7E009E");
//		this.grd1.addColorStop(1, "#FF56B8");
//	} else if (this.series==2) {
//		this.grd1.addColorStop(0, "#00245E");
//		this.grd1.addColorStop(1, "#7E009E");
//		this.grd2.addColorStop(0, "#7E009E");
//		this.grd2.addColorStop(1, "#FF56B8");
//	} else {
//		this.grd1.addColorStop(0, "#00245E");
//		this.grd1.addColorStop(1, "#540C88");
//		this.grd2.addColorStop(0, "#540C88");
//		this.grd2.addColorStop(0.5, "#7E009E");
//		this.grd2.addColorStop(1, "#A91CA6");
//		this.grd3.addColorStop(0, "#A91CA6");
//		this.grd3.addColorStop(1, "#FF56B8");
//	}
	// cyan gradients
	if (this.series==1) {
		// mid: #1C5A89
		this.grd1.addColorStop(0, "#01042C");
		this.grd1.addColorStop(0.333, "#152F5F");
		this.grd1.addColorStop(0.666, "#2386B4");
		this.grd1.addColorStop(1, "#04ECF1");
	} else if (this.series==2) {
		// mid: #0E204E
		this.grd1.addColorStop(0, "#01042C");
		this.grd1.addColorStop(0.666, "#152F5F");
		this.grd1.addColorStop(1, "#1C5A89");
		// mid: #18A8C8
		this.grd2.addColorStop(0, "#1C5A89");
		this.grd2.addColorStop(0.333, "#2386B4");
		this.grd2.addColorStop(1, "#04ECF1");
	} else {
		// mid: #0B1945
		this.grd1.addColorStop(0, "#01042C");
		this.grd1.addColorStop(1, "#152F5F");
		// mid: #1C5A89
		this.grd2.addColorStop(0, "#152F5F");
		this.grd2.addColorStop(1, "#2386B4");
		// mid: #13B9D2
		this.grd3.addColorStop(0, "#2386B4");
		this.grd3.addColorStop(1, "#04ECF1");
	}
}
AcidChart.prototype.draw_chart_data = function() {
	var i;
	var cchartdata = this.chartdata[this.range]
	this.ctxc.fillStyle = "#bbdbe5"; //transparent ok: #b0d0da
	this.ctxc.fillRect(0,this.chartoffy,this.chartx,this.charty);
	if (this.series>2) {
		for (i=0 ; i<this.chartx ; i++) {
			if (typeof cchartdata.dataarr1[i]=="number" && typeof cchartdata.dataarr2[i]=="number" && typeof cchartdata.dataarr3[i]=="number") {
				this.ctxc.fillStyle = this.grd3;
				this.ctxc.fillRect(this.chartx-i-1,this.charty+this.chartoffy,1,-Math.ceil((cchartdata.dataarr1[i]+cchartdata.dataarr2[i]+cchartdata.dataarr3[i])*this.charty/this.dmax));
				this.chartxdata = i;
			} else {
				this.ctxc.fillStyle = this.grdnull;
				this.ctxc.fillRect(this.chartx-i-1,this.chartoffy,1,this.charty);
			}
		}
	}
	if (this.series>1) {
		for (i=0 ; i<this.chartx ; i++) {
			if (typeof cchartdata.dataarr1[i]=="number" && typeof cchartdata.dataarr2[i]=="number") {
				this.ctxc.fillStyle = this.grd2;
				this.ctxc.fillRect(this.chartx-i-1,this.charty+this.chartoffy,1,-Math.ceil(((cchartdata.dataarr1[i]+cchartdata.dataarr2[i])*this.charty/this.dmax)));
				this.chartxdata = i;
			} else {
				this.ctxc.fillStyle = this.grdnull;
				this.ctxc.fillRect(this.chartx-i-1,this.chartoffy,1,this.charty);
			}
		}
	}
	for (i=0 ; i<this.chartx ; i++) {
		if (typeof cchartdata.dataarr1[i]=="number") {
			this.ctxc.fillStyle = this.grd1;
			this.ctxc.fillRect(this.chartx-i-1,this.charty+this.chartoffy,1,-Math.ceil(cchartdata.dataarr1[i]*this.charty/this.dmax));
			this.chartxdata = i;
		} else {
			this.ctxc.fillStyle = this.grdnull;
			this.ctxc.fillRect(this.chartx-i-1,this.chartoffy,1,this.charty);
		}
	}
	// do not use real chartxdata - allow scrolling even when there are no data
	this.chartxdata = this.chartx;
	this.maxposition = this.chartxdata - this.elemc.clientWidth;
}
AcidChart.prototype.draw_frame = function() {
	this.ctxc.beginPath();
	this.ctxc.strokeStyle = "#000000";
	this.ctxc.setLineDash([]);
	this.ctxc.moveTo(0,this.charty+0.5+this.chartoffy);
	this.ctxc.lineTo(this.chartx,this.charty+0.5+this.chartoffy);
	this.ctxc.stroke();

	this.ctxl.beginPath();
	this.ctxl.strokeStyle = "#000000";
	this.ctxl.setLineDash([]);
	this.ctxl.moveTo(this.chartoffx-0.5,this.chartoffy+this.charty+3);
	this.ctxl.lineTo(this.chartoffx-0.5,this.chartoffy-3);
	this.ctxl.stroke();

	this.ctxr.beginPath();
	this.ctxr.strokeStyle = "#000000";
	this.ctxr.setLineDash([]);
	this.ctxr.moveTo(0.5,this.chartoffy+this.charty+3);
	this.ctxr.lineTo(0.5,this.chartoffy-3);
	this.ctxr.stroke();
}
AcidChart.prototype.draw_vaux_r0 = function() {
	var tsdata_begin = new Date((this.chartdata[0].timestamp-this.chartx*60)*1000);
	var tsoffset = 59 - tsdata_begin.getUTCMinutes();
	var tshour = tsdata_begin.getUTCHours();
	var i;

	this.hoff = tsoffset%2;
	for (i=tsoffset-60 ; i<this.chartx+60 ; i+=60) {
		this.ctxc.beginPath();
		this.ctxc.strokeStyle = "#000000";
		this.ctxc.setLineDash([]);
		this.ctxc.moveTo(i+0.5,this.chartoffy+this.charty+3);
		this.ctxc.lineTo(i+0.5,this.chartoffy+this.charty);
		this.ctxc.stroke();
		this.ctxc.drawStringAt(i-14,this.chartoffy+this.charty+4,this.format_hour(tshour));
		this.ctxc.beginPath();
		this.ctxc.strokeStyle = "#000000";
		if ((tshour%6)==0) {
			this.ctxc.setLineDash([3,1]);
			this.ctxc.lineDashOffset = 2;
		} else {
			this.ctxc.setLineDash([1,1]);
			this.ctxc.lineDashOffset = 1;
		}
		this.ctxc.moveTo(i+0.5,this.chartoffy+this.charty);
		this.ctxc.lineTo(i+0.5,this.chartoffy);
		this.ctxc.stroke();
		tshour+=1;
		if (tshour==24) {
			tshour=0;
		}
	}
}
AcidChart.prototype.draw_vaux_r1 = function() {
	var tsdata_begin = new Date((this.chartdata[1].timestamp-this.chartx*360-3600)*1000);
	var tsoffset = 9 - Math.floor(tsdata_begin.getUTCMinutes()/6);
	var tshour = tsdata_begin.getUTCHours();
	var i;

	this.hoff = tsoffset%2;
	for (i=tsoffset-20 ; i<this.chartx+20 ; i+=10) {
		if ((tshour%6)==0) {
			this.ctxc.beginPath();
			this.ctxc.strokeStyle = "#000000";
			this.ctxc.setLineDash([]);
			this.ctxc.moveTo(i+0.5,this.chartoffy+this.charty+3);
			this.ctxc.lineTo(i+0.5,this.chartoffy+this.charty);
			this.ctxc.stroke();
			this.ctxc.drawStringAt(i-14,this.chartoffy+this.charty+4,this.format_hour(tshour));
		}
		this.ctxc.beginPath();
		this.ctxc.strokeStyle = "#000000";
		if (tshour==0) {
			this.ctxc.setLineDash([3,1]);
			this.ctxc.lineDashOffset = 2;
		} else if ((tshour%6)==0) {
			this.ctxc.setLineDash([1,1]);
			this.ctxc.lineDashOffset = 1;
		} else {
			this.ctxc.setLineDash([1,3]);
			this.ctxc.lineDashOffset = 1;
		}
		this.ctxc.moveTo(i+0.5,this.chartoffy+this.charty);
		this.ctxc.lineTo(i+0.5,this.chartoffy);
		this.ctxc.stroke();
		tshour+=1;
		if (tshour==24) {
			tshour=0;
		}
	}
}
AcidChart.prototype.draw_vaux_r2 = function() {
	var tsdata_begin = new Date((this.chartdata[2].timestamp-this.chartx*1800-43200)*1000);
	var tsoffset = 11 - Math.floor((tsdata_begin.getUTCMinutes()+60*(tsdata_begin.getUTCHours()%6))/30);
	var tshour = Math.floor(tsdata_begin.getUTCHours()/6)*6;
	var tsday = tsdata_begin.getUTCDate()-1; // 0..30
	var tsmonth = tsdata_begin.getUTCMonth(); // 0..11
	var tsyear = tsdata_begin.getUTCFullYear(); // YYYY

	this.hoff = tsoffset%2;
	for (i=tsoffset-24 ; i<this.chartx+24 ; i+=12) {
		tshour+=6;
		if (tshour==24) {
			tshour=0;
			tsday+=1;
			if (tsday==this.month_days(tsyear,tsmonth)) {
				tsday = 0;
				tsmonth+=1;
				if (tsmonth==12) {
					tsmonth = 0;
					tsyear+=1;
				}
			}
		}
		if (tshour==0) {
			this.ctxc.beginPath();
			this.ctxc.strokeStyle = "#000000";
			this.ctxc.setLineDash([]);
			this.ctxc.moveTo(i+0.5,this.chartoffy+this.charty+3);
			this.ctxc.lineTo(i+0.5,this.chartoffy+this.charty);
			this.ctxc.stroke();
		}
		if (tshour==12) {
			this.ctxc.drawStringAt(i-14,this.chartoffy+this.charty+4,this.format_day(tsmonth+1,tsday+1));
		}
		this.ctxc.beginPath();
		this.ctxc.strokeStyle = "#000000";
		if (tsday==0 && tshour==0) {
			this.ctxc.setLineDash([3,1]);
			this.ctxc.lineDashOffset = 2;
		} else if (tshour==0) {
			this.ctxc.setLineDash([1,1]);
			this.ctxc.lineDashOffset = 1;
		} else {
			this.ctxc.setLineDash([1,3]);
			this.ctxc.lineDashOffset = 1;
		}
		this.ctxc.moveTo(i+0.5,this.chartoffy+this.charty);
		this.ctxc.lineTo(i+0.5,this.chartoffy);
		this.ctxc.stroke();
	}
}
AcidChart.prototype.draw_vaux_r3 = function() {
	var tsdata_begin = new Date((this.chartdata[3].timestamp-this.chartx*86400)*1000);
	var tsmonth = tsdata_begin.getUTCMonth();
	var tsyear = tsdata_begin.getUTCFullYear();
	var tsoffset = this.month_days(tsyear,tsmonth) - tsdata_begin.getUTCDate();

	this.hoff = 0;
	for (i=tsoffset ; i<this.chartx+31 ; i+=this.month_days(tsyear,tsmonth)) {
		this.ctxc.drawStringAt(i-5-Math.floor(this.month_days(tsyear,tsmonth)/2),this.chartoffy+this.charty+4,this.format_month(tsmonth+1));
		this.ctxc.beginPath();
		this.ctxc.strokeStyle = "#000000";
		this.ctxc.setLineDash([]);
		this.ctxc.moveTo(i+0.5,this.chartoffy+this.charty+3);
		this.ctxc.lineTo(i+0.5,this.chartoffy+this.charty);
		this.ctxc.stroke();
		this.ctxc.beginPath();
		this.ctxc.strokeStyle = "#000000";
		if (tsmonth==11) {
			this.ctxc.setLineDash([3,1]);
			this.ctxc.lineDashOffset = 2;
		} else {
			this.ctxc.setLineDash([1,1]);
			this.ctxc.lineDashOffset = 1;
		}
		this.ctxc.moveTo(i+0.5,this.chartoffy+this.charty);
		this.ctxc.lineTo(i+0.5,this.chartoffy);
		this.ctxc.stroke();
		tsmonth+=1;
		if (tsmonth==12) {
			tsmonth = 0;
			tsyear+=1;
		}
	}
}
AcidChart.prototype.draw_vaux = function() {
	if (this.range==0) {
		this.draw_vaux_r0();
	} else if (this.range==1) {
		this.draw_vaux_r1();
	} else if (this.range==2) {
		this.draw_vaux_r2();
	} else if (this.range==3) {
		this.draw_vaux_r3();
	}
}
AcidChart.prototype.draw_haux = function() {
	var lshift;
	var i;

	if (this.chartdata.percent) {
		this.suffix += "%";
	}
	lshift = 6*this.suffix.length;
	// draw horizontal aux lines
	for (i=0 ; i<=this.charty ; i+=20) {
		this.ctxl.drawStringAt(this.chartoffx-28-lshift,this.chartoffy+this.charty-i-3,this.format_number(this.minval,this.mode)+this.suffix);
		this.minval += this.step;
		this.ctxl.beginPath();
		this.ctxl.strokeStyle = "#000000";
		this.ctxl.setLineDash([]);
		this.ctxl.moveTo(this.chartoffx-3,i+0.5+this.chartoffy);
		this.ctxl.lineTo(this.chartoffx,i+0.5+this.chartoffy);
		this.ctxl.stroke();
		if (i<this.charty) {
			this.ctxc.beginPath();
			this.ctxc.strokeStyle = "#000000";
			this.ctxc.setLineDash([1,1]);
			this.ctxc.lineDashOffset = this.hoff;
			this.ctxc.moveTo(0,i+0.5+this.chartoffy);
			this.ctxc.lineTo(this.chartx,i+0.5+this.chartoffy);
			this.ctxc.stroke();
		} else {
			this.ctxr.beginPath();
			this.ctxr.strokeStyle = "#000000";
			this.ctxr.setLineDash([]);
			this.ctxr.moveTo(0.5,i+0.5+this.chartoffy);
			this.ctxr.lineTo(3.5,i+0.5+this.chartoffy);
			this.ctxr.stroke();
		}
	}
}
AcidChart.prototype.find_max = function(from,to) {
	var m,v,i;
	var cchartdata = this.chartdata[this.range]
	m = 0;
	for (i=from ; i<to ; i++) {
		v = 0;
		if (typeof cchartdata.dataarr1[i] == "number") {
			v = cchartdata.dataarr1[i];
		}
		if (this.series>1) {
			if (typeof cchartdata.dataarr2[i] == "number") {
				v += cchartdata.dataarr2[i];
			}
		}
		if (this.series>2) {
			if (typeof cchartdata.dataarr3[i] == "number") {
				v += cchartdata.dataarr3[i];
			}
		}
		if (v > m) {
			m = v;
		}
	}
	this.max = m;
}
AcidChart.prototype.fix_max = function() {
	var ypts;
	var mode;
	var step,dmax,m,scale;
	var mult,smult;
	var cchartdata = this.chartdata[this.range]

	ypts = Math.floor(this.charty/20);

	this.minval = 0;

	m = this.max;
	m *= cchartdata.multiplier;
	m /= cchartdata.divisor;
	if (m==0) {
		m = 1;
	}
	m *= 100;
	mode = 0;
	scale = 0;
	mult = 0.01;
	smult = 0.01;
	while (1) {
		step = Math.ceil(m/ypts);
		dmax = step * ypts;
		//console.log(`m: ${m} ; dmax: ${dmax} ; step: ${step} ; mode: ${mode} ; scale: ${scale} ; mult: ${mult} ; smult: ${smult}`);
		if (dmax < 1000) {
			this.step = step * smult;
			this.dmax = ((dmax * mult) * cchartdata.divisor) / cchartdata.multiplier;
			this.mode = mode;
			this.scale = scale;
			return;
		}
		m/=10;
		mult*=10;
		smult*=10;
		if (mode<2) {
			mode++;
		} else {
			mode = 0;
			smult = 0.01;
			scale++;
		}
	}
}
AcidChart.prototype.find_scale = function() {
	var suffixes = "um kMGTPEZY";
	var scale;

	scale = this.scale + 2 + this.chartdata.basescale;
	if (scale==2) {
		this.suffix = "";
	} else {
		this.suffix = suffixes.charAt(scale);
	}
}
AcidChart.prototype.check_chartdata = function() {
	var i;
	var cchartdata = this.chartdata[this.range];
	if (typeof cchartdata.dataarr1 == "undefined") {
		this.series = 0;
		this.values = 0;
	} else if (typeof cchartdata.dataarr2 == "undefined") {
		this.series = 1;
		this.values = cchartdata.dataarr1.length;
	} else if (typeof cchartdata.dataarr3 == "undefined") {
		this.series = 2;
		this.values = cchartdata.dataarr1.length;
		if (cchartdata.dataarr2.length < this.values) {
			this.values = cchartdata.dataarr2.length;
		}
	} else {
		this.series = 3;
		this.values = cchartdata.dataarr1.length;
		if (cchartdata.dataarr2.length < this.values) {
			this.values = cchartdata.dataarr2.length;
		}
		if (cchartdata.dataarr3.length < this.values) {
			this.values = cchartdata.dataarr3.length;
		}
	}
}
AcidChart.prototype.analyse_data = function() {
	if (this.chartdata) {
		this.check_chartdata();
		this.find_max(0,this.chartx);
	}
}
AcidChart.prototype.number_to_string = function(number) {
	var str = number.toFixed(0);
	var res = "";

	while (str!="") {
		if (res!="") {
			res = str.slice(-3) + " " + res;
		} else {
			res = str.slice(-3);
		}
		str = str.slice(0,-3);
	}
	return res;
}
AcidChart.prototype.get_tooltip_date_r0 = function(chartindx) {
	var tsdata = new Date((this.chartdata[0].timestamp-chartindx*60)*1000);
	var tsmin = tsdata.getUTCMinutes()
	var tshour = tsdata.getUTCHours();
	var tsday = tsdata.getUTCDate();
	var tsmonth = tsdata.getUTCMonth();
	var tsyear = tsdata.getUTCFullYear();
	return tsyear + "." + this.format_day(tsmonth+1,tsday) + " " + this.format_month(tshour) + ":" + this.format_month(tsmin) + " (1 minute)";
}
AcidChart.prototype.get_tooltip_date_r1 = function(chartindx) {
	var tsdata = new Date((this.chartdata[1].timestamp-chartindx*360)*1000);
	var tsmin = tsdata.getUTCMinutes()
	var tshour = tsdata.getUTCHours();
	var tsday = tsdata.getUTCDate();
	var tsmonth = tsdata.getUTCMonth();
	var tsyear = tsdata.getUTCFullYear();
	return tsyear + "." + this.format_day(tsmonth+1,tsday) + " " + this.format_month(tshour) + ":" + this.format_month(tsmin) + " (6 minutes)";
}
AcidChart.prototype.get_tooltip_date_r2 = function(chartindx) {
	var tsdata = new Date((this.chartdata[2].timestamp-chartindx*1800)*1000);
	var tsmin = tsdata.getUTCMinutes()
	var tshour = tsdata.getUTCHours();
	var tsday = tsdata.getUTCDate();
	var tsmonth = tsdata.getUTCMonth();
	var tsyear = tsdata.getUTCFullYear();
	return tsyear + "." + this.format_day(tsmonth+1,tsday) + " " + this.format_month(tshour) + ":" + this.format_month(tsmin) + " (30 minutes)";
}
AcidChart.prototype.get_tooltip_date_r3 = function(chartindx) {
	var tsdata = new Date((this.chartdata[3].timestamp-chartindx*86400)*1000);
	var tsyear = tsdata.getUTCFullYear().toString();
	var tsmonth = tsdata.getUTCMonth();
	var tsday = tsdata.getUTCDate();
	return tsyear + "." + this.format_day(tsmonth+1,tsday) + " (1 day)";
}
AcidChart.prototype.get_tooltip_date = function(chartindx) {
	if (this.range==0) {
		return this.get_tooltip_date_r0(chartindx);
	} else if (this.range==1) {
		return this.get_tooltip_date_r1(chartindx);
	} else if (this.range==2) {
		return this.get_tooltip_date_r2(chartindx);
	} else if (this.range==3) {
		return this.get_tooltip_date_r3(chartindx);
	}
	return "???";
}
AcidChart.prototype.get_tooltip_data = function(offset) {
	var chartindx;
	var cchartdata;
	var value1,value2,value3,vsum;
	var suffix;
	var ret;
	var color1,color2,color3;
	var nodata;

	if (this.chartdata) {
		chartindx = offset + this.position;

		ret = this.get_tooltip_date(chartindx) + ": ";

		color1 = "red";
		color2 = "red";
		color3 = "red";
		if (this.series==1) {
			color1 = "#1C5A89";
		} else if (this.series==2) {
			color1 = "#0E204E";
			color2 = "#18A8C8";
		} else if (this.series==3) {
			color1 = "#0B1945";
			color2 = "#1C5A89";
			color3 = "#13B9D2";
		}
		cchartdata = this.chartdata[this.range];
		nodata = 0;
		value1 = 0;
		value2 = 0;
		value3 = 0;
		if (this.series>0) {
			if (typeof cchartdata.dataarr1[chartindx]=="number") {
				value1 = cchartdata.dataarr1[chartindx];
			} else {
				nodata = 1;
			}
		}
		if (this.series>1) {
			if (typeof cchartdata.dataarr2[chartindx]=="number") {
				value2 = cchartdata.dataarr2[chartindx];
			} else {
				nodata = 1;
			}
		}
		if (this.series>2) {
			if (typeof cchartdata.dataarr3[chartindx]=="number") {
				value3 = cchartdata.dataarr3[chartindx];
			} else {
				nodata = 1;
			}
		}
		if (nodata) {
			return ret + "no data";
		}
		if (this.chartdata.percent) {
			suffix = "%";
		} else {
			suffix = "";
		}

		if (cchartdata.multiplier==1 && cchartdata.divisor==1 && this.chartdata.basescale==0) {
			vsum = value1 + value2 + value3;
			if (this.series>0) {
				ret += "<span style='color:" + color1 + "'>" + this.number_to_string(value1) + suffix + "</span>";
			}
			if (this.series>1) {
				ret += " + <span style='color:" + color2 + "'>" + this.number_to_string(value2) + suffix + "</span>";
			}
			if (this.series>2) {
				ret += " + <span style='color:" + color3 + "'>" + this.number_to_string(value3) + suffix + "</span>";
			}
			if (this.series>1) {
				ret += " = " + this.number_to_string(vsum) + suffix;
			}
		} else {
			value1 *= cchartdata.multiplier;
			value1 /= cchartdata.divisor;
			value1 *= Math.pow(1000,this.chartdata.basescale);
			value2 *= cchartdata.multiplier;
			value2 /= cchartdata.divisor;
			value2 *= Math.pow(1000,this.chartdata.basescale);
			value3 *= cchartdata.multiplier;
			value3 /= cchartdata.divisor;
			value3 *= Math.pow(1000,this.chartdata.basescale);
			vsum = value1 + value2 + value3;
			if (this.series>0) {
				ret += "<span style='color:" + color1 + "'>" + value1.toFixed(4) + suffix + "</span>";
			}
			if (this.series>1) {
				ret += " + <span style='color:" + color2 + "'>" + value2.toFixed(4) + suffix + "</span>";
			}
			if (this.series>2) {
				ret += " + <span style='color:" + color3 + "'>" + value3.toFixed(4) + suffix + "</span>";
			}
			if (this.series>1) {
				ret += " = " + vsum.toFixed(4) + suffix;
			}
		}
		return ret;
	} else {
		return "";
	}
}
AcidChart.prototype.handle_common_scale = function() {
	var i,ac,chartlist;
	var max = 0;
	if (this.common_scale) {
		chartlist = window.acid_chart_groups[this.chartgroup];

		for (i=0 ; i<chartlist.length ; i++) {
			ac = chartlist[i];
			if (ac.chartdata) {
				if (ac.max > max) {
					max = ac.max;
				}
			}
		}
		for (i=0 ; i<chartlist.length ; i++) {
			ac = chartlist[i];
			if (ac.chartdata) {
				ac.max = max;
			}
		}
	}
	this.fix_max();
	this.find_scale();
}
AcidChart.prototype.whole_group_loaded = function() {
	var i,ac,chartlist;
	chartlist = window.acid_chart_groups[this.chartgroup];
	for (i=0 ; i<chartlist.length ; i++) {
		ac = chartlist[i];
		if (ac.loading) {
			return 0;
		}
	}
	return 1;
}
AcidChart.prototype.draw_chart = function() {
	var i,ac,chartlist;
	if (this.common_scale) {
		if (this.whole_group_loaded()) {
			chartlist = window.acid_chart_groups[this.chartgroup];
			for (i=0 ; i<chartlist.length ; i++) {
				ac = chartlist[i];
				if (ac.draw_chart_delayed) {
					ac.draw_chart_delayed = 0;
					ac.delayed_draw();
				}
			}
			this.delayed_draw();
		} else {
			this.draw_chart_delayed = 1;
			return;
		}
	} else {
		this.delayed_draw();
	}
}
AcidChart.prototype.delayed_draw = function() {
	var ac = this;
	window.setTimeout(function() { ac.do_draw_chart(); },0);
}
AcidChart.prototype.do_draw_chart = function() {
	if (this.chartdata) {
		this.handle_common_scale();
		this.ctxl.clearRect(0,0,this.canvasl.width,this.canvasl.height);
		this.ctxc.clearRect(0,0,this.canvasc.width,this.canvasc.height);
		this.ctxr.clearRect(0,0,this.canvasr.width,this.canvasr.height);
		this.create_gradients();
		this.draw_chart_data();
		this.draw_frame();
		this.draw_vaux();
		this.draw_haux();
		this.eleml.style.background = "transparent url(" + this.canvasl.toDataURL() + ") no-repeat left center";
		this.elemc.style.background = "transparent url(" + this.canvasc.toDataURL() + ") no-repeat right -" + this.position + "px center";
		this.elemr.style.background = "transparent url(" + this.canvasr.toDataURL() + ") no-repeat left center";
	} else {
		this.eleml.style.background = "transparent";
		this.elemc.style.background = "transparent url('data:image/svg+xml,%3Csvg%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%3E%3Ctext%20style%3D%22white-space%3A%20pre%3B%20fill%3A%20rgb(180%2C%2051%2C%2051)%3B%20font-family%3A%20Arial%2C%20sans-serif%3B%20font-size%3A%2028px%3B%22%20x%3D%220.0%22%20y%3D%2280.0%22%3ELoading error%3C%2Ftext%3E%3C%2Fsvg%3E')";
		this.elemr.style.background = "transparent";
	}
}
AcidChart.prototype.redraw = function() {
	var chartheight = this.elemc.clientHeight - 20;
	chartheight = Math.floor(chartheight/20)*20;
	this.canvasl.height = chartheight + 20;
	this.canvasr.height = chartheight + 20;
	this.canvasc.height = chartheight + 20;
	this.charty = chartheight;
	this.draw_chart();
}
AcidChart.prototype.set_range = function(range) {
	this.range = range;
	this.position = 0;
}
AcidChart.prototype.get_data_url = function(part) {
	if (part==0) {
		return this.canvasl.toDataUrl();
	}
	if (part==1) {
		return this.canvasc.toDataUrl();
	}
	if (part==2) {
		return this.canvasr.toDataUrl();
	}
}
AcidChart.prototype.set_pos = function(newposition,store) {
	if (this.chartdata) {
		this.elemc.style.backgroundPosition = "right -" + newposition + "px center";
	}
	if (store) {
		this.position = newposition;
	}
}
AcidChart.prototype.handle_move_by = function(deltax,store) {
	var newposition;
	var chartlist;
	var i;
	newposition = this.position + deltax;
	if (newposition > this.maxposition) {
		newposition = this.maxposition;
	}
	if (newposition < 0) {
		newposition = 0;
	}
	if (this.movegroup) {
		chartlist = window.acid_chart_groups[this.chartgroup];
		for (i=0 ; i<chartlist.length ; i++) {
			chartlist[i].set_pos(newposition,store);
		}
	} else {
		this.set_pos(newposition,store);
	}
}
AcidChart.prototype.mouse_up_handler = function(ev) {
	if (document.current_acid_chart) {
		var ac = document.current_acid_chart;
		ac.handle_move_by(ev.clientX - ac.mousex,1);
		if (document.removeEventListener) {
			ac.elemc.removeEventListener('mousemove',ac.mouse_move_handler,false);
			document.removeEventListener('mouseup',ac.mouse_up_handler,false);
		} else if (document.detachEvent) {
			ac.elemc.detachEvent('onmousemove',ac.mouse_move_handler);
			document.detachEvent('onmouseup',ac.mouse_up_handler);
		}
	}
	delete document.current_acid_chart;
}
AcidChart.prototype.mouse_move_handler = function(ev) {
	if (document.current_acid_chart) {
		var ac = document.current_acid_chart;
		ac.handle_move_by(ev.clientX - ac.mousex,0);
		if (ev.stopPropagation) ev.stopPropagation();
		if (ev.preventDefault) ev.preventDefault();
		ev.cancelBubble=true;
		ev.returnValue=false;
		return false;
	}
}
AcidChart.prototype.mouse_down_handler = function(ev) {
	var ac = this.acid_chart;
	if (ac.chartdata) {
		if (ev.shiftKey) {
			ac.movegroup = 0;
		} else {
			ac.movegroup = 1;
		}
		ac.mousex = ev.clientX;
		ac.maxposition = ac.chartxdata - this.clientWidth;
		document.current_acid_chart = ac;
		if (document.addEventListener) {
			ac.elemc.addEventListener('mousemove',ac.mouse_move_handler,false);
			document.addEventListener('mouseup',ac.mouse_up_handler,false);
		} else if (document.attachEvent) {
			ac.elemc.attachEvent('onmousemove',ac.mouse_move_handler);
			document.attachEvent('onmouseup',ac.mouse_up_handler);
		}
		if (ev.stopPropagation) ev.stopPropagation();
		if (ev.preventDefault) ev.preventDefault();
		ev.cancelBubble=true;
		ev.returnValue=false;
		return false;
	}
}
AcidChart.prototype.window_resize_handler = function(ev) {
	var ac,i;
	for (i=0 ; i<window.acid_charts.length ; i++) {
		ac = window.acid_charts[i];
		if (ac.chartdata) {
			ac.maxposition = ac.chartxdata - ac.elemc.clientWidth;
			ac.movegroup = 0;
			ac.handle_move_by(0,1);
		}
	}
}
AcidChart.prototype.click_handler = function(ev) {
	var ac = this.acid_chart;
	var newheight;
	if (ac.chartdata) {
		ac.hmode = ac.hmode+1;
		if (ac.hmode==4) {
			ac.hmode = 0;
		}
		newheight = ac.heightbase * Math.pow(2,ac.hmode) + ac.heightdiff;
		ac.elemp.style.height = "" + newheight + "px";
		ac.redraw();
	}
}
AcidChart.prototype.load_data = function(url, full_reload=true) {
	var request = new XMLHttpRequest();
	var ac = this;
	this.elemc.style.cursor = "progress";
	this.loading = 1;
	this.position = 0;
	this.chartdata = null;
	if (full_reload) {
		this.eleml.style.background = "transparent";
		this.elemc.style.background = "transparent url('data:image/svg+xml,%3Csvg%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%3E%3Ctext%20style%3D%22white-space%3A%20pre%3B%20fill%3A%20rgb(51%2C%2051%2C%2051)%3B%20font-family%3A%20Arial%2C%20sans-serif%3B%20font-size%3A%2028px%3B%22%20x%3D%220.0%22%20y%3D%2280.0%22%3ELoading...%3C%2Ftext%3E%3C%2Fsvg%3E')";
		this.elemr.style.background = "transparent";
	}
	request.open('GET',url);
	request.responseType = 'json';
	request.onload = function() {
		ac.loading = 0;
		if (request.response && typeof request.response.basescale == "number") {
			ac.chartdata = request.response;
			ac.elemc.style.cursor = "grab";
		} else {
			ac.elemc.style.cursor = "default";
			ac.eleml.style.cursor = "default";
		}
		ac.analyse_data();
		ac.draw_chart();
	}
	request.onerror = function() {
		ac.loading = 0;
		ac.elemc.style.cursor = "default";
		ac.eleml.style.cursor = "default";
	}
	request.onabort = function() {
		ac.loading = 0;
		ac.elemc.style.cursor = "default";
		ac.eleml.style.cursor = "default";
	}
	request.send();
}


function AcidChartWrapper(idprefix,chartgroup,host,port,mode,id,show_loading=true) {
	this.host = host;
	this.port = port;
	this.mode = mode;
	this.id = id;
	this.ac = new AcidChart(idprefix,chartgroup);
	this.reload(show_loading);
}
AcidChartWrapper.prototype.set_range = function(newrange) {
	var newrange = Math.floor(newrange);
	if (newrange<0) {
		newrange = 0;
	}
	if (newrange>3) {
		newrange = 3;
	}
	this.ac.set_range(newrange);
}
AcidChartWrapper.prototype.set_id = function(newid) {
	this.id = Math.floor(newid);
	this.reload();
}
AcidChartWrapper.prototype.set_host_and_port = function(newhost,newport,newmode) {
	this.host = newhost;
	this.port = newport;
	this.mode = newmode;
	this.reload();
}
AcidChartWrapper.prototype.set_host_port_id = function(newhost,newport,newmode,newid) {
	this.host = newhost;
	this.port = newport;
	this.mode = newmode;
	this.id = Math.floor(newid);
	this.reload();
}
AcidChartWrapper.prototype.reload = function(full_reload=true) {
	var url;
	url = "chartdata.cgi?host=" + this.host + "&port=" + this.port + "&mode=" + this.mode + "&id=" + (this.id * 10 + 9);
	this.ac.load_data(url, full_reload);
}

function AcidChartSetRange(chartgroup,newrange) {
	var newrange = Math.floor(newrange);
	var chartlist;
	var i;

	if (typeof window.acid_chart_groups == "undefined") {
		return;
	}
	if (typeof window.acid_chart_groups[chartgroup] == "undefined") {
		return;
	}

	chartlist = window.acid_chart_groups[chartgroup];

	if (newrange<0) {
		newrange = 0;
	}
	if (newrange>3) {
		newrange = 3;
	}

	for (i=0 ; i<chartlist.length ; i++) {
		chartlist[i].set_range(newrange);
	}
	for (i=0 ; i<chartlist.length ; i++) {
		chartlist[i].analyse_data();
	}
	for (i=0 ; i<chartlist.length ; i++) {
		chartlist[i].redraw();
	}
}

function AcidChartSetCommonScale(chartgroup,setflag) {
	var newrange = Math.floor(newrange);
	var chartlist;
	var i;

	if (typeof window.acid_chart_groups == "undefined") {
		return;
	}
	if (typeof window.acid_chart_groups[chartgroup] == "undefined") {
		return;
	}

	chartlist = window.acid_chart_groups[chartgroup];

	for (i=0 ; i<chartlist.length ; i++) {
		chartlist[i].analyse_data();
		chartlist[i].common_scale = setflag;
	}
	for (i=0 ; i<chartlist.length ; i++) {
		chartlist[i].redraw();
	}
}
