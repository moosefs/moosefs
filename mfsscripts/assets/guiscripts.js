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

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('theme-toggle').addEventListener('click', onClickTheme, false);
});
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
  document.getElementById('auto-refresh').addEventListener('click', onClickAutoRefresh, false);
  document.getElementById('refresh-button').addEventListener('click', onClickRefresh, false);
  setInterval(autoRefresh, 1000);
}, false);

// if (document.addEventListener) {
//   document.getElementById('auto-refresh').addEventListener('click', onClickAutoRefresh, false);
//   document.getElementById('refresh-button').addEventListener('click', onClickRefresh, false);
// }

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
  // document.documentElement.style.setProperty('--progress-rotation', rotation + "deg");
  refreshRequired = doRefreshCountdowns();
  if ((countUp == 0 && checkbox && checkbox.checked) || refreshRequired) {
    doRefresh();
  }
}


function refreshWithNewParam(param, newParamValue) {
  doRefresh(false, param, newParamValue);
}

function doRefresh(scrollToGraphInfo=false, param=null, newParamValue=null) {
  // secondsSinceLastRefresh = (Date.now() - lastRefresh.getTime())/1000;

  //refresh master charts if they exist, reload data but don't reload div-s
  if (typeof ma_charttab !== 'undefined') {
    initAllKnobs();
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
      initTagsTooltips();
    } 
    else if (this.readyState == 4){
      //unsuccessful ajax request, report footer error icon
      const elTimestamp = document.querySelector('#refresh-timestamp');
      if (elTimestamp) {
        elTimestamp.innerHTML = '<span class="ERROR" data-tt="gui_refresh_failed">failed</span>';
        initTagsTooltips();
      }
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
    if (param!=null && newParamValue!=null) {
      const url = new URL(href);
      url.searchParams.set(param, newParamValue); 
      href = url.href;
    }
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
    //First check if the knob has a data-selected-rotation attribute ()
    degrees=knob.getAttribute("data-selected-rotation")
    if (degrees==null) {
      //if not, find stored knob rotation in the session storage
      if (typeof sessionStorage != "undefined") { //read rotation from the storage...
        var val=sessionStorage.getItem(id+"-knob-rotation");
        if (val!=null) {
          degrees=parseInt(val); //...if it is stored
        }
      }
    }
    if (degrees==null) {//load default if not found in storage
      degrees=knob.getAttribute("data-initial-rotation");
      if (degrees!=null) {
        degrees=parseInt(degrees);
      }
    }
    if (degrees!=NaN)
      rotateKnob(id, degrees)
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
  const knob=document.getElementById(id);
  knob.setAttribute("data-selected-rotation", degrees)  
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
function showGraphInfo(section, add_params='') {
  const graphInfo=document.getElementById('mfsgraph-info');
  if (!graphInfo) return;
  const href = new URL(window.location.href);
  const params = new URLSearchParams(href.search);
  params.set('ajax', 'container');
  params.set('sections', section);
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
        // tbl.style.width = 'min-content';
        tbl.classList.add('narrow'); //try to squeeze the table as much as possible (cells like "progbar" will be squeezed)
        tbl.style.marginLeft = '0';
      });
      applyAcidTab(false, true); //update dynamic tables after reload (styling, #-numbering, sorting etc)
      initAllKnobs(); //re-init knobs positions
      initTagsTooltips();
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
      if (href && href!="null") {
        refreshGraphInfo(href);
      }
    }
  }
}
// Graph support
/////////////////////////////////////////

/////////////////////////////////////////
// Tooltips management
let tooltipMap = {}; //global - map of all tooltips (id -> {title, description, solution})
clickHideInactive = false;
mouseOverTooltip = false;
mouseOverElement = false;
showHelpTooltips = initShowHelpTooltips(); // Shall we show help tooltips? Info, warning, and error are shown regardless

document.addEventListener('DOMContentLoaded', function() {
  document.getElementById('tt-onoff').addEventListener('click', onClickTooltipsOnOff, false);
  // Call the function with the path to your tooltips JSON file
  loadTooltipDescriptions('/../assets/help.json').then( tooltipMap => {
    initTooltipsStack();
    initTagsTooltips();
  });
}, false);

function onClickTooltipsOnOff(e) {
  showHelpTooltips = !showHelpTooltips;
  localStorage.setItem('showHelpTooltips', showHelpTooltips?'1':'0');
}

function initShowHelpTooltips() {
  showHelpTooltips = !(localStorage.getItem('showHelpTooltips')==='0');
  const checkbox=document.getElementById('tt-onoff');
  if (checkbox) {
    checkbox.checked = showHelpTooltips;
  }
  return showHelpTooltips;
}

// Function to load tooltip descriptions from a JSON file
async function loadTooltipDescriptions(url) {
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    const data = await response.json();
    Object.keys(data).forEach(tooltip_id => {
      const tooltip = data[tooltip_id];
      if (tooltip.title) {
        tooltipMap[tooltip_id] = {
          severity: tooltip.severity,
          title: tooltip.title.endsWith('.') ? tooltip.title.slice(0, -1) : tooltip.title,
          description: (tooltip.description) ? tooltip.description.replace('Note','<br/>Note') : null,
          solution: tooltip.solution
        };
      };
    });
    return tooltipMap;
  } catch (error) {
    console.error('Failed to load JSON data:', error);
    return {};
  }
}

function html_icon(severity, scale=1.0) {
  if (severity.toLowerCase()=='error') id='icon-error'
  else if (severity.toLowerCase()=='warning') id='icon-warning'
  else id='icon-info'
  return `<span class="icon"><svg height="${12*scale}px" width="${12*scale}px"><use transform="scale(${scale})" xlink:href="#${id}"/></svg></span>`
}

function getTooltipContent(element) {
  let tooltipsList = [];
  const tooltipIds = element.getAttribute('data-tt')
  if (tooltipIds) {
    // Split the comma-separated list of tooltip IDs and add them to the tooltipsList
    tooltipIds.split(',').forEach(tooltipId => {
      tooltip=tooltipMap[tooltipId];
      if (!tooltip) {
        tooltip = {
          severity: "Info",
          title: "Missing tooltip details",
          description: "Missing details for this tooltip: <b>"+tooltipId+"</b>.",
          solution: "Please report it to MooseFS support."
        };
      }
      tooltipsList.push(tooltip);
    });
  }
  // Add inline tooltip details if they exist
  inline = element.getAttribute('data-tt-error');
  if (inline) 
    inlineSeverity = "Error";
  else {
    inline = element.getAttribute('data-tt-warning');
    if (inline) 
      inlineSeverity = "Warning";
    else {
      inline = element.getAttribute('data-tt-info');
      if (inline) 
        inlineSeverity = "Info";
      else {
        inline = element.getAttribute('data-tt-help');
        if (inline) 
          inlineSeverity = "Help";
        else {
          inlineSeverity = null;
        }
      }
    }
  }   

  if (inlineSeverity && inline) {
    tooltipsList.push({
      severity: inlineSeverity,
      title: inline,
      description: "",
      solution: ""
    });
  }
  if (!showHelpTooltips) {
    tooltipsList = tooltipsList.filter(tooltip => tooltip.severity != "Help");
  }
  if (tooltipsList.length == 0) return '';

  html_tt = '';
  const sortOrder = ["Error", "Warning", "Info", "Help"];
  tooltipsList.sort((a, b) => {
    const indexA = sortOrder.indexOf(a.severity);
    const indexB = sortOrder.indexOf(b.severity);
    return indexA - indexB;
  });
  useAccordion = (tooltipsList.length > 1);
  firstActive = 'active';
  if (useAccordion) html_tt += `<div class="accordion">`;
  tooltipsList.forEach(tooltipDetails => {
    if (useAccordion) html_tt += `<div class="accordion-item ${firstActive}">`
    html_tt += `<div class="tooltip-title-row accordion-header">`;
    if (tooltipDetails.severity!='Help' && (tooltipDetails.description || tooltipDetails.solution)) 
      html_tt += `<div class="tooltip-title-icon">${html_icon(tooltipDetails.severity, 1.2)}</div>`;
    if (tooltipDetails.description) html_tt += `<div class="tooltip-title">${tooltipDetails.title}</div>`;
    if (useAccordion) html_tt += `<svg class="accordion-icon" height="12px" width="12px"><use xlink:href="#icon-chevron"/></svg>`;
    html_tt += `</div>`;
    if (useAccordion) html_tt += `<div class="accordion-content">`;
    if (tooltipDetails.description) 
      html_tt += `<div class="tooltip-description"><hr class="title-bar">${tooltipDetails.description}</div>`
    else
      html_tt += `<div class="tooltip-description">${tooltipDetails.title}</div>`
    if (tooltipDetails.solution) html_tt += `<div class="tooltip-solution"><b>Fix:</b> ${tooltipDetails.solution}</div>`
    if (useAccordion) html_tt += `</div>`; //accordion-content
    if (useAccordion) html_tt += `</div>`; //accordion-item 
    firstActive = '';
  });
  if (useAccordion) html_tt += `</div>`; //accordion
  return html_tt;
}

// Function to initialize tooltips stack: document-wide event listeners etc.
function initTooltipsStack() {
  const tooltip = document.getElementById('tooltip');
  document.addEventListener('click', (event) => {
    if (!clickHideInactive && tooltip.classList.contains('locked') && tooltip.classList.contains('tooltip-visible')) {
      tooltip.classList.remove('locked');
      if (hideTooltip()) {
        // event.preventDefault(); 
        event.stopPropagation();
      }
    }
  });

  tooltip.addEventListener('mouseenter', (event) => { mouseOverTooltip = true;});
  tooltip.addEventListener('mouseleave', (event) => { mouseOverTooltip = false; hideTooltip();});
  tooltip.addEventListener('click', (event) => {
    if (!tooltip.classList.contains('locked')) tooltip.classList.add('locked'); 
    event.stopPropagation();
  });
}

//Initializes individual html tag tooltips event listeners etc.
function initTagsTooltips() {
  const tooltip = document.getElementById('tooltip');
  document.querySelectorAll('[data-tt], [data-tt-help], [data-tt-info], [data-tt-warning], [data-tt-error]').forEach(element => {
    element.addEventListener('mouseenter', (event) => { mouseOverElement=true; showTooltip(event, element);});
    // element.addEventListener('mousemove', (event) => { showTooltip(event, element);});
    element.addEventListener('mouseleave', (event) => { 
      setTimeout(() => { if (!mouseOverTooltip && !mouseOverElement) hideTooltip(); }, 200);
      mouseOverElement = false;
    });

    element.addEventListener('click', (event) => {
      if (tooltip.classList.contains('locked')) {
        tooltip.classList.remove('locked');
        hideTooltip();
      } else {
        //uncomment for on-click tooltip open
        // clickHideInactive = true;
        // tooltip.classList.add('locked');
        // showTooltip(event, element);
        // setTimeout(() => {clickHideInactive =false; }, 1000);
      }
    });
  });
}

function showTooltip(event, element){
  const tooltip = document.getElementById('tooltip');
  //don't show or change tooltip if visible and locked
  if (tooltip.classList.contains('locked') && tooltip.classList.contains('tooltip-visible')) return;
  tooltip.innerHTML = getTooltipContent(element);
  if (tooltip.innerHTML == '') {
    hideTooltip(); //sometimes delayed tooltip is shown, so hide it if there is no content
    return;
  }
  positionTooltip(event, element);
  tooltip.classList.add('tooltip-visible');

  const accordionHeaders = document.querySelectorAll('.accordion-header');
  accordionHeaders.forEach(header => {
      header.addEventListener('click', () => {
          const accordionItem = header.parentElement; 
          const isActive = accordionItem.classList.contains('active');
          document.querySelectorAll('.accordion-item.active').forEach(item => {
              item.classList.remove('active'); // Remove active class from all items
          });
          // Toggle current item
          if (!isActive) accordionItem.classList.add('active');
      });
  });
};

function hideTooltip() {
  const tooltip = document.getElementById('tooltip');
  if (!tooltip.classList.contains('locked')) {
      tooltip.classList.remove('tooltip-visible');
      tooltip.classList.remove('positioned');
      return true;
  }
  return false;
};

function positionTooltip(event, element) {
  const offset = 10;
  const tooltip = document.getElementById('tooltip');
  const tooltipRect = tooltip.getBoundingClientRect();
  const viewportWidth = window.innerWidth;
  // const viewportHeight = window.innerHeight;

  // Calc element position
  const elementRect = element.getBoundingClientRect();
  let top = elementRect.top - tooltipRect.height - offset; // Over the element
  let left = elementRect.left + (elementRect.width - tooltipRect.width) / 2; // Center horizontally

  // Do we fit the viewport?
  if (top < 0) { 
    top = elementRect.bottom; // Place under the element
  }

  // Adjust left to ensure the tooltip stays within the viewport
  if (left + tooltipRect.width > viewportWidth) {
    left = viewportWidth - tooltipRect.width - offset;
  } else if (left < 0) {
    left = offset; 
  }

  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${top}px`;
  tooltip.style.bottom = null;
  tooltip.style.right = null;
}
// Tooltips management
/////////////////////////////////////////

/////////////////////////////////////////
// Hamburger menu
function toggleHamburger()
{
  const menu = document.getElementById("hamburger-menu-content");
  menu.classList.toggle("active");
}
// Hamburger menu
/////////////////////////////////////////
