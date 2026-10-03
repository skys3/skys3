# The SDK matrix's CLI client (tests/sdk/run.sh).
FROM amazon/aws-cli:2.37.8@sha256:420ab345e847291b541b45d989535f55bcff957c27fa1100fae4aa233e86398c
# The test presigns a GET and fetches it with curl.
RUN command -v curl || (yum install -y curl && yum clean all)
COPY sdk-test.sh /client/sdk-test.sh
ENTRYPOINT ["sh", "/client/sdk-test.sh"]
