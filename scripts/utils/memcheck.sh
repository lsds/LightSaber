# pass a name as a parameter
while true; do ps -eo rss,pid,euser,args:100 --sort %mem | grep -v grep | grep -i $@ | awk '{printf $1/1024 "MB"; $1=""; print }' ; sleep 0.5s ; done
