FROM @BASE_IMAGE_NAME@
LABEL maintainer="CMS Build"
LABEL name="CMS Worker Node on EL - Buildtime dependencies"

# Dependencies on libidn libXmu libstdc++-static might be cleaned up in the future
RUN dnf install --disablerepo=epel -y perl-Carp perl-PathTools perl-Data-Dumper perl-Errno perl-Exporter \
	perl-File-Path perl-File-Temp perl-Getopt-Long perl-IO perl-Text-ParseWords perl-constant \
	perl-Digest-MD5 perl-PathTools perl-Data-Dumper perl-Digest-MD5 perl-Exporter perl-File-Path \
	perl-File-Temp perl-Getopt-Long perl-File-Which perl-Scalar-List-Utils perl-Term-ANSIColor \
	perl-CGI perl-DBI perl-Encode perl-Memoize perl-Storable perl-Time-HiRes perl-Time-Local \
	perl-YAML perl-IO-Compress rpm-build git nspr nss rsync wget openssl-devel ncurses-devel \
	libX11-devel readline-devel tcl-devel tk-devel mesa-libGLU-devel libXext-devel libXft-devel \
	libXpm-devel java-1.8.0-openjdk-devel java-11-openjdk-devel java-17-openjdk libtool libdrm \
        gcc-c++ libidn libXmu python3-cryptography jq &&\
	dnf clean all

# Please add user packages in the RUN command below
RUN dnf install --disablerepo=epel -y man-db time bc nano strace perf xauth vim cmake \
        krb5-workstation glibc-langpack-en environment-modules xdg-utils squid tmux &&\
	dnf clean all

RUN dnf --enablerepo=powertools install -y texinfo libstdc++-static &&\
	epel_pkgs="@EPEL_PACKAGES@" &&\
	if [ -n "$epel_pkgs" ] ; then dnf install -y $epel_pkgs; fi &&\
	xcmd="@EXTRA_COMMAND@" &&\
	if [ "$xcmd" = "" ] ; then xcmd=true; fi &&\
	eval $xcmd &&\
	dnf update -y ca-certificates &&\
	dnf clean all &&\
	/root/fix_ssh_config.sh /etc/ssh/ssh_config &&\
	echo "Timestamp: @BUILD_DATE@" > /image-build-info.txt

LABEL build-date="@BUILD_DATE@"
