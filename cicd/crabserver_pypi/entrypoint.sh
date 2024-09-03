#!/bin/bash
# Original script from https://github.com/dmwm/CMSKubernetes/blob/2b0454f9205cb8f97fecb91bf6661b59e4b31424/docker/crabserver/run.sh
# Should belong to CMSKubernetes.

if [ -f /etc/robots/robotkey.pem ]; then
    sudo cp /etc/robots/robotkey.pem /data/srv/current/auth/crabserver/dmwm-service-key.pem
    sudo cp /etc/robots/robotcert.pem /data/srv/current/auth/crabserver/dmwm-service-cert.pem
    sudo chown $USER:$USER /data/srv/current/auth/crabserver/dmwm-service-key.pem
    sudo chown $USER:$USER /data/srv/current/auth/crabserver/dmwm-service-cert.pem
fi

if [ -f /etc/hmac/hmac ]; then
    sudo mkdir -p /data/srv/current/auth/wmcore-auth
    if [ -f /data/srv/current/auth/wmcore-auth/header-auth-key ]; then
        sudo rm /data/srv/current/auth/wmcore-auth/header-auth-key
    fi
    sudo cp /etc/hmac/hmac /data/srv/current/auth/wmcore-auth/header-auth-key
    sudo chown $USER.$USER /data/srv/current/auth/wmcore-auth/header-auth-key
    sudo mkdir -p /data/srv/current/auth/crabserver
    if [ -f /data/srv/current/auth/crabserver/header-auth-key ]; then
        sudo rm /data/srv/current/auth/crabserver/header-auth-key
    fi
    sudo cp /etc/hmac/hmac /data/srv/current/auth/crabserver/header-auth-key
    sudo chown $USER.$USER /data/srv/current/auth/crabserver/header-auth-key
fi

if [ -f /etc/proxy/proxy ]; then
    export X509_USER_PROXY=/etc/proxy/proxy
    sudo mkdir -p /data/srv/state/crabserver/proxy
    if [ -f /data/srv/state/crabserver/proxy/proxy.cert ]; then
        rm /data/srv/state/crabserver/proxy/proxy.cert
    fi
    sudo ln -s /etc/proxy/proxy /data/srv/state/crabserver/proxy/proxy.cert
    sudo mkdir -p /data/srv/current/auth/proxy
    if [ -f /data/srv/current/auth/proxy/proxy ]; then
        rm /data/srv/current/auth/proxy/proxy
    fi
    sudo ln -s /etc/proxy/proxy /data/srv/current/auth/proxy/proxy
fi

if [ -f /etc/secrets/config.py ]; then
    sudo cp /etc/secrets/config.py /data/srv/current/config/crabserver/config.py
    sudo chown $USER:$USER /data/srv/current/config/crabserver/config.py
fi

if [ -f /etc/secrets/CRABServerAuth.py ]; then
    sudo cp /etc/secrets/CRABServerAuth.py /data/srv/current/auth/crabserver/CRABServerAuth.py
    sudo chown $USER:$USER /data/srv/current/auth/crabserver/CRABServerAuth.py
fi


exec /usr/bin/tini -- "$@"
