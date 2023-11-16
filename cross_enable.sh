#!/usr/bin/env bash

cat debian/control | sed 's/\(^Build-Depends:.*\), python (>= 2.5) | python3/\1/' > debian/control_cross
mv debian/control_cross debian/control
