for i in {1..100}; do
	go test -run 2A
	if [ -z $? ]; then
		exit 1
	fi
done

