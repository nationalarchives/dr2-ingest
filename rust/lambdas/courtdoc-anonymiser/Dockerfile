FROM alpine
COPY target/x86_64-unknown-linux-musl/release/anonymiser .
RUN apk add --no-cache wget=1.25.0-r1 && \
	  mkdir /input /output && \
      chown 1000:1000 /input /output
USER 1000:1000
CMD ["./anonymiser", "--input", "/input", "--output", "/output"]
