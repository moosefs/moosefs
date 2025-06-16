#!/usr/bin/env bash

cat debian/control | sed 's/\(^Build-Depends:.*\), python3 (>= 3.4)/\1/' > debian/control_cross
mv debian/control_cross debian/control
