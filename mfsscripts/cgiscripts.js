function getOffset( el ) {
  var _x = 0;
  var _y = 0;
  while( el && !isNaN( el.offsetLeft ) && !isNaN( el.offsetTop ) ) {
    _x += el.offsetLeft - el.scrollLeft;
    _y += el.offsetTop - el.scrollTop;
    el = el.offsetParent;
    }
  return { top: _y, left: _x };
}

function showToolTip(e) {
  var t = document.getElementById('charttooltip');
  var hu = document.getElementById('charthlineup');
  var hd = document.getElementById('charthlinedown');
  var ac = e.target.acid_chart;
  var bodyrect,elemrect,offset;
  var uh,dh,eltop;
  var tdata;

  if (e.target.classList.contains('CHARTJSC') && typeof(ac)=='object') {
    t.style.display = "block";
    hu.style.display = "block";
    hd.style.display = "block";
  } else {
    t.style.display = "none";
    hu.style.display = "none";
    hd.style.display = "none";
    return;
  }
  bodyrect = document.body.getBoundingClientRect();
  elemrect = e.target.getBoundingClientRect();
  offset = e.target.clientWidth - (e.clientX - elemrect.left) - 1;
  tdata = ac.get_tooltip_data(offset);
  t.innerHTML = tdata;

  if (tdata=="") {
    t.style.display = "none";
    hu.style.display = "none";
    hd.style.display = "none";
    return;
  }

  eltop = (elemrect.top - bodyrect.top);
  uh = (e.pageY - eltop - 1);
  dh = (e.target.clientHeight - uh - 2);
  hu.style.left = e.pageX + "px";
  hu.style.top = eltop + "px";
  hu.style.height = uh + "px";
  hd.style.left = e.pageX + "px";
  hd.style.top = (e.pageY + 2) + "px";
  hd.style.height = dh + "px";

  if (e.clientX < bodyrect.width/2) {
    t.style.left = e.pageX + 12 + "px";
    t.style.top = e.pageY + 0 + "px";
  } else {
    t.style.left = e.pageX - 16 - t.clientWidth + "px";
    t.style.top = e.pageY + 0 + "px";
  }
}

function hideToolTip(e) {
  var t = document.getElementById('charttooltip');
  var hu = document.getElementById('charthlineup');
  var hd = document.getElementById('charthlinedown');

  t.style.display = "none";
  hu.style.display = "none";
  hd.style.display = "none";
  return;
}

if (document.addEventListener) {
  document.body.addEventListener('mousemove',showToolTip,false);
  document.body.addEventListener('mousedown',hideToolTip,false);
  document.body.addEventListener('click',hideToolTip,false);
  document.body.addEventListener('dblclick',hideToolTip,false);
} else if (document.attachEvent) {
  document.body.attachEvent('onmousemove',showToolTip);
  document.body.attachEvent('onmousedown',hideToolTip);
  document.body.attachEvent('onclick',hideToolTip);
  document.body.attachEvent('ondblclick',hideToolTip);
}

/////////////////////////////////////////
// Light/dark mode switching
const toggleSwitchIcon = document.querySelector('#theme-toggle use');

if (localStorage.getItem('theme')==='light') {
  switchTheme('light');
}

function switchTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  localStorage.setItem('theme', theme);
  if (theme === 'dark') {
    toggleSwitchIcon.setAttribute('xlink:href', '#icon-light');
  } else {
    toggleSwitchIcon.setAttribute('xlink:href', '#icon-dark');
  }
}

function onClickTheme(e) {    
  if (toggleSwitchIcon.getAttribute('xlink:href').endsWith('icon-dark')) {
    switchTheme('dark');
  }
  else {        
    switchTheme('light');
  }
}

if (document.addEventListener) {
  document.getElementById('theme-toggle').addEventListener('click', onClickTheme, false);
}
// End of light/dark mode switching
/////////////////////////////////////////

/////////////////////////////////////////
// Auto refreshing
let countUp=0;
let refreshPeriod=60; //default refresh period in seconds
let lastRefresh = new Date();

function formattedDateTime(date) {
  function pad0(n) {return ("0" + n).slice (-2)}
  return `${date.getFullYear()}-${pad0(date.getMonth() + 1)}-${date.getDate()} ${pad0(date.getHours())}:${pad0(date.getMinutes())}:${pad0(date.getSeconds())}`;
}

document.addEventListener('DOMContentLoaded', function() {
  document.getElementById('refresh-timestamp').innerHTML = formattedDateTime(lastRefresh);
  const urlParams = new URLSearchParams(window.location.search);

  allowAutoRefresh=true;
  if (urlParams.has('refresh')) {
    refreshPeriod=parseInt(urlParams.get('refresh'));
    if (isNaN(refreshPeriod)) refreshPeriod=0;
    refreshPeriod=Math.max(refreshPeriod,0);
    refreshPeriod=Math.min(refreshPeriod,999);
    localStorage.setItem('auto-refresh', refreshPeriod>0?'1':'0');
  }
  if (refreshPeriod==0) allowAutoRefresh=false;

  checkbox=document.getElementById('auto-refresh');
  if (checkbox) {
    checkbox.checked = ((allowAutoRefresh && localStorage.getItem('auto-refresh')==='1')?true:false);
    if (allowAutoRefresh) 
      document.querySelector('#auto-refresh + .slider .slider-text').innerText=(refreshPeriod-countUp).toString();
  else
      document.getElementById('auto-refresh-slide').classList.add('hidden');
    
  }
  setInterval(autoRefresh, 1000);
}, false);

if (document.addEventListener) {
  document.getElementById('auto-refresh').addEventListener('click', onClickAutoRefresh, false);
  document.getElementById('refresh-button').addEventListener('click', onClickRefresh, false);
}

function onClickAutoRefresh(e) {    
  const checkbox=document.getElementById('auto-refresh');
  if (checkbox) {
    localStorage.setItem('auto-refresh', checkbox.checked?'1':'0');
    autoRefresh();
  }
}

function onClickRefresh(e) {    
  doRefresh();
}

function autoRefresh() {
  countUp = (countUp==refreshPeriod-1) ? 0 : countUp+1;
  const checkbox=document.getElementById('auto-refresh');
  if (checkbox && !checkbox.checked) countUp=0;
  if (refreshPeriod>0) {
    rotation=(360 * countUp/refreshPeriod - 180);
    document.querySelector('#auto-refresh + .slider .slider-text').innerText=(refreshPeriod-countUp).toString();
  } else {
    rotation = -180;
    document.querySelector('#auto-refresh + .slider .slider-text').innerText='';
  }
  document.documentElement.style.setProperty('--progress-rotation', rotation + "deg");
  if (countUp != 0) return;
  if (checkbox && checkbox.checked) {
    doRefresh();
  }
}

function doRefresh() {
  // secondsSinceLastRefresh = (Date.now() - lastRefresh.getTime())/1000;

  //refresh master charts if they exist, reload data but don't reload divs
  if (typeof ma_charttab !== 'undefined') {
    for (const chartWrapper of ma_charttab) {
      chartWrapper.reload(false);
    }
  }
  if (typeof ma_chartcmp !== 'undefined') {
    for (const chartWrapper of ma_chartcmp) {
      chartWrapper.reload(false);
    }
  }  
  //refresh cs charts if they exist, reload data but don't reload divs
  if (typeof cs_charttab !== 'undefined') {
    for (const chartWrapper of cs_charttab) {
      chartWrapper.reload(false);
    }
  }
  if (typeof cs_chartcmp !== 'undefined') {
    for (const chartWrapper of cs_chartcmp) {
      chartWrapper.reload(false);
    }
  }
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (this.readyState == 4 && this.status == 200) {
      lastRefresh = new Date();
      document.getElementById('container-ajax').innerHTML = this.responseText;
      document.getElementById('refresh-timestamp').innerHTML = formattedDateTime(lastRefresh);
      applyAcidTab(false, true); //update dynamic tables after reload (styling, #-numbering, sorting etc)
      initAllKnobs(); //re-init knobs positions
    }
  };
  href=window.location.href;
  if (href.includes('mfs.cgi?')) {
    href=href.replace('mfs.cgi?', 'mfs.cgi?ajax=container&');
  } else if (href.includes('mfs.cgi')) {
    href=href.replace('mfs.cgi', 'mfs.cgi?ajax=container');
  } else {
    href = null;
  }
  if (href) {
    xhttp.open("GET", href, true);
    xhttp.send();
  }
}
// End of auto refreshing
/////////////////////////////////////////

/////////////////////////////////////////
// Knobs management
document.addEventListener('DOMContentLoaded', function() {
  initAllKnobs();
}, false);

function initAllKnobs() {
  var knobs = document.querySelectorAll(".knob-arrow"), i;
  for (i = 0; i < knobs.length; ++i) {
    initKnob(knobs[i].id);
  }
}

//loads knob rotation from the session storage 
function initKnob(id) {
  if (id!="") {
    knob=document.getElementById(id);
    degrees=null;
    //initial knob rotation
    if (typeof sessionStorage != "undefined") { //read rotation from the storage...
      var val=sessionStorage.getItem(id+"-knob-rotation");
      if (val!=null) {
        degrees=parseInt(val); //...if it is stored
        document.documentElement.style.setProperty("--"+id+"-knob-rotation", degrees + "deg");
      }
    }
    if (degrees==null) {//load default if not found in storage
      degrees=knob.getAttribute("data-initial-rotation");
      if (degrees!=null) {
        degrees=parseInt(degrees);
        if (degrees!=NaN) 
          rotateKnob(id, degrees);
      }
    }
    // setTimeout((knob_) => {
    //   // knob_.style.visibility="visible";
    //   knob_.style.opacity=1;
    // }, 500,knob);
  }
}
//rotates a knob to the desired (degrees) position and stores its rotation to the session storage
function rotateKnob(id, degrees) {
  document.documentElement.style.setProperty("--"+id+"-knob-rotation", degrees + "deg");
  storeKnobRotation(id,degrees);
}

//rotates a knob by one position and stores its rotation to the session storage
function rotateNextKnob(id, degrees_arr, fun_arr) {
  degrees=getComputedStyle(document.body).getPropertyValue("--"+id+"-knob-rotation");
  if (degrees==null) return;
  degrees=degrees.replace("deg","");
  degrees=parseInt(degrees);
  index=0;
  if (degrees!=null) index=degrees_arr.indexOf(degrees);
  index=(index+1)%degrees_arr.length
  eval(fun_arr[index]);
  rotateKnob(id, degrees_arr[index]);
}

//store knob rotation to the session storage
function storeKnobRotation(id, degrees) {
  if (typeof sessionStorage != "undefined" && !document.getElementById(id).classList.contains("dont-store")) { 
    sessionStorage.setItem(id+"-knob-rotation", degrees);
  }
}
// Knobs management
/////////////////////////////////////////
