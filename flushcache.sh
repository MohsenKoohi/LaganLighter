#!/bin/bash
#
# This script is called to remove storage data that are cached by OS while loading the graph
#
# Having a sudo access to the machine with a Linux distribution,  
# the /drop_caches.sh file can be created by running the following three steps
# 	sudo echo -e '#!/bin/bash\n\necho 3 > /proc/sys/vm/drop_caches\n' > /drop_caches.sh
# 	sudo chmod a+x /drop_caches.sh
# 	sudo echo -e 'ALL  ALL = (ALL) NOPASSWD: /drop_caches.sh\n' >> /etc/sudoers
#
# [1] https://www.kernel.org/doc/Documentation/sysctl/vm.txt
#
# If you have created a similar script, you may add its path as a new line to the COMMS variable
#
# If none of the script exist, `flushcache` file in `paragrapher/test` is called
# which may take a longer time to finish

COMMS=`echo -e "
	/drop_caches.sh
	/var/shared/power/bin/flushcache.sh
	/opt/service/bin/dropcache both
	$(realpath ~)/Programs/scripts/drop_caches.sh
"`

while IFS= read -r c; do
	c=`echo "$c" | xargs`
	[ -z "$c" ] && continue
	file=`echo "$c"|cut -f1 -d' '`
	if [ -f $file ]; then
		$c
		[ $? = 0 ] && exit

		sudo $c
		[ $? = 0 ] && exit
	fi
done <<< $COMMS
exit
make flushcache -C paragrapher/test
