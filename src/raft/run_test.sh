for i in {1..100}; do
	go test -run 2C
	rc=$?
	if [ $rc -ne 0 ]; then
		exit 1
	fi
done

