all: 
	gofmt -w -tabindent=false -tabwidth=4 _includes/examples/
	make -C _includes/examples/simple
	make -C pkg/

clean:
	make -C _includes/examples/simple clean
