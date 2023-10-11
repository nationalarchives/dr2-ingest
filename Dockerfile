FROM alpine
COPY target/x86_64-unknown-linux-musl/release/anonymiser .
RUN apk add --no-cache wget
CMD ./anonymiser --input /input --output /output
