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
  refreshRequired = doRefreshCountdowns();
  if ((countUp == 0 && checkbox && checkbox.checked) || refreshRequired) {
    doRefresh();
  }
}

function doRefresh(scrollToGraphInfo=false) {
  // secondsSinceLastRefresh = (Date.now() - lastRefresh.getTime())/1000;

  //refresh master charts if they exist, reload data but don't reload div-s
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
  //refresh cs charts if they exist, reload data but don't reload div-s
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
      const graph_info_old = document.getElementById('mfsgraph-info');
      lastRefresh = new Date();
      document.getElementById('container-ajax').innerHTML = this.responseText; //refresh the main container
      graph_info_new = document.getElementById('mfsgraph-info');
      if (graph_info_new && graph_info_old) graph_info_new.innerHTML = graph_info_old.innerHTML; //use old graph-info for a while to prevent blinking
      document.getElementById('refresh-timestamp').innerHTML = formattedDateTime(lastRefresh); //refresh footer timestamp
      graph_info_href = sessionStorage.getItem("mfsgraph-info-data-ajax-href");
      if (graph_info_new && graph_info_href) refreshGraphInfo(graph_info_href, scrollToGraphInfo); //refresh graph info
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

//auto refresh also takes care for refreshing countdowns, this function performs actual countdown
//returns true if countdown finished to 0, so the general page refresh is required
function doRefreshCountdowns() {
  var refreshRequired = false;
  var countdowns = document.querySelectorAll(".countdown"), i;
  for (i = 0; i < countdowns.length; ++i) {
    if (countdowns[i].innerText.includes(':')) {
      const parts = countdowns[i].innerText.split(':'); // Split the string into minutes and seconds
      var minutes = parseInt(parts[0], 10); // Parse the minutes part
      var seconds = (minutes * 60) + parseInt(parts[1], 10); // Parse the seconds part
      seconds = seconds - 1;
      if (seconds>0) {
        minutes = Math.floor(seconds / 60); // Calculate the number of complete minutes
        seconds = seconds % 60; // Calculate the remaining seconds
        // Format minutes and seconds to always show two digits
        const formattedMinutes = String(minutes);
        const formattedSeconds = String(seconds).padStart(2, '0');
        countdowns[i].innerText = `${formattedMinutes}:${formattedSeconds}`;
      } else {
        countdowns[i].innerText = "0:00";
        refreshRequired = true; //countdown completed, general refresh will be required
      }
    }
  }
  return refreshRequired;
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

/////////////////////////////////////////
// Graph support
document.addEventListener('DOMContentLoaded', function() {
  initGraphInfo();
}, false);

//Shows an additional table under the graph after a click on the graph element with more details on this element
function showGraphInfo(section, subsection='', add_params='') {
  const graphInfo=document.getElementById('mfsgraph-info');
  if (!graphInfo) return;
  const href = new URL(window.location.href);
  const params = new URLSearchParams(href.search);
  params.set('ajax', 'container');
  params.set('sections', section);
  params.set('subsections', subsection);
  params.set('readonly','1');
  params.set('selectable','0');
  params.delete('HDdata'); //ajax-ed HDdata may interfere with the main HDdata (possibly included in the original URL)
  href.search=params.toString()+'&'+add_params;
  if (href) {
    sessionStorage.setItem("mfsgraph-info-data-ajax-href", href);
  }
  //refresh the whole page and scroll to graph info
  doRefresh(true);
}

function refreshGraphInfo(href, scrollTo=false) {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (this.readyState == 4 && this.status == 200) {
      document.getElementById('mfsgraph-info').innerHTML = this.responseText;
      // add scroll-up and close table buttons to the table
      var tables = document.querySelectorAll('#mfsgraph-info .tab_title');
      for (var i = 0; i < tables.length; i++) {
        var span = document.createElement("span");
        span.innerHTML = "&nbsp;&nbsp;&nbsp;&nbsp;<svg height='12px' width='12px' class='pointer' onclick='scrollToGraph()'><use xlink:href='#icon-go-up'/></svg>&nbsp;&nbsp;<svg height='12px' width='12px' class='pointer' onclick='hideGraphInfo()'><use xlink:href='#icon-close'/></svg>";
        tables[i].appendChild(span);
      }
      var tables = document.querySelectorAll('#mfsgraph-info table.acid_tab');
      // Loop through all tables and make them as narrow as possible
      tables.forEach(function(tbl) {
        tbl.style.width = 'min-content';
        tbl.style.minWidth = '500px';
        tbl.style.marginLeft = '0';
    });
      applyAcidTab(false, true); //update dynamic tables after reload (styling, #-numbering, sorting etc)
      initAllKnobs(); //re-init knobs positions
      if (scrollTo) scrollToGraphInfo();
    }
  };
  const graphInfo=document.getElementById('mfsgraph-info');
  if (graphInfo){
    if (href) {
      xhttp.open("GET", href, true);
      xhttp.send();
      sessionStorage.setItem("mfsgraph-info-data-ajax-href", href);
    }
  }
}

//Hides additional graph information
function hideGraphInfo() {
  sessionStorage.setItem("mfsgraph-info-data-ajax-href", null);
  const graphInfo=document.getElementById('mfsgraph-info');
  if (graphInfo) {
    graphInfo.innerHTML = '';
    graphInfo.setAttribute('data-ajax-href','');
    scrollToGraph();
  }
}

//Scrolls screen to show graph's top
function scrollToGraph() {
  if (document.getElementById('mfsgraph-tile')) document.getElementById('mfsgraph-tile').scrollIntoView({ behavior: 'smooth' });
}

function scrollToGraphInfo() {
  if (document.getElementById('mfsgraph-info')) document.getElementById('mfsgraph-info').scrollIntoView({ behavior: 'smooth' });
}

function initGraphInfo() {
  //show graph info upon page reload if there is appropriate href stored in the session storage
  const graphInfo=document.getElementById('mfsgraph-info');
  if (graphInfo) {
    var href=graphInfo.getAttribute('data-ajax-href');
    if (!href) {
      href = sessionStorage.getItem("mfsgraph-info-data-ajax-href");
      if (href) {
        refreshGraphInfo(href);
      }
    }
  }
}
// Graph support
/////////////////////////////////////////
