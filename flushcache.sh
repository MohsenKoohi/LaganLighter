#!/bin/sh

# This script is called to remove storage data that are cached by OS
#
# Having a sudo access to the machine with a Linux distribution, 
# the /drop_caches.sh file can be created by running the following three steps
# 	sudo echo -e '#!/bin/bash\n\necho 3 > /proc/sys/vm/drop_caches' > /drop_caches.sh
# 	sudo chmod a+x /drop_caches.sh
# 	sudo echo -e 'ALL  ALL = (ALL) NOPASSWD: /drop_caches.sh\n' >> /etc/sudoers
#
# If you have created a similar script, you may add it as a new line to the COMMS variable
#
# If none of the script exist, `flushcache` file in `paragrapher/test` is called

COMMS="
	/var/shared/power/bin/flushcache.sh
	/drop_caches.sh

"

for c in $COMMS; do
	if [ -f $c ]; then
		$c
		if [ $? = 0 ]; then
			exit
		fi

		sudo $c
		if [ $? = 0 ]; then
			exit
		fi
	fi
done

make flushcache -C paragrapher/test
