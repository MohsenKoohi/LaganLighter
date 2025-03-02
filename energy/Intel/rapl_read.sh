#!/bin/sh

printf '%-20s; %-20s; %-15s; %20s; %20s\n' "name" "socket:domain_id"  "domain" "energy_uj"  "max_energy_uj"
for f in `find /sys/class/powercap/intel-rapl\:* | grep -P "\d+"`; do
    # echo $f;
    name=`echo $f | rev | cut -d/ -f1 | rev`
    id=`echo $name | cut -d: -f2,3`;
    domain=`cat $f/name`
    energy=`cat $f/energy_uj`     # needs sudo
    max_energy=`cat $f/max_energy_range_uj`
    printf '%-20s; %-20s; %-15s; %20s; %20s\n' ${name} ${id} ${domain} ${energy} ${max_energy}
done


# Jacob Output
# name                ; socket:domain_id    ; domain         ;            energy_uj;        max_energy_uj 
# intel-rapl:0        ; 0                   ; package-0      ;           5597181429;          65532610987 
# intel-rapl:0:0      ; 0:0                 ; core           ;          17890384969;          65532610987 
# intel-rapl:0:1      ; 0:1                 ; dram           ;          42654180108;          65532610987 
# intel-rapl:1        ; 1                   ; package-1      ;          44097140474;          65532610987 
# intel-rapl:1:0      ; 1:0                 ; core           ;          28312441531;          65532610987 
# intel-rapl:1:1      ; 1:1                 ; dram           ;          28336196162;          65532610987 
# intel-rapl:2        ; 2                   ; package-2      ;          10537557287;          65532610987 
# intel-rapl:2:0      ; 2:0                 ; core           ;          64874120580;          65532610987 
# intel-rapl:2:1      ; 2:1                 ; dram           ;          11793304602;          65532610987 
# intel-rapl:3        ; 3                   ; package-3      ;           1800898200;          65532610987 
# intel-rapl:3:0      ; 3:0                 ; core           ;          19178661532;          65532610987 
# intel-rapl:3:1      ; 3:1                 ; dram           ;          17701212944;          65532610987

# smp01
# name                ; socket:domain_id    ; domain         ;            energy_uj;        max_energy_uj
# intel-rapl:0        ; 0                   ; package-3      ;         226618687799;         262143328850
# intel-rapl:0:0      ; 0:0                 ; dram           ;          60279115781;          65712999613
# intel-rapl:1        ; 1                   ; package-2      ;         181795825861;         262143328850
# intel-rapl:1:0      ; 1:0                 ; dram           ;          58966008642;          65712999613
# intel-rapl:2        ; 2                   ; package-1      ;          35231629289;         262143328850
# intel-rapl:2:0      ; 2:0                 ; dram           ;          32651449214;          65712999613
# intel-rapl:3        ; 3                   ; package-0      ;         143013545932;         262143328850
# intel-rapl:3:0      ; 3:0                 ; dram           ;          26530978739;          65712999613


# smp04
# name                ; socket:domain_id    ; domain         ;            energy_uj;        max_energy_uj
# intel-rapl:0        ; 0                   ; package-1      ;         176399656753;         262143328850
# intel-rapl:1        ; 1                   ; package-0      ;         147773556513;         262143328850


# smp05  
# name                ; socket:domain_id    ; domain         ;            energy_uj;        max_energy_uj
# intel-rapl:0        ; 0                   ; package-1      ;         118155980417;         262143328850
# intel-rapl:1        ; 1                   ; package-0      ;         206448289179;         262143328850

