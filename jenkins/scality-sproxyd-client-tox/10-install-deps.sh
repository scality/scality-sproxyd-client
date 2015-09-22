#!/bin/bash -xue

function install_deb {
    sudo aptitude install -y python-dev libffi-dev gcc
}

function install_centos {
    # GCC installed automatically in debian because of recommended packages
    # Doing that manually in centos.
    sudo yum -y install python-devel libffi-devel epel-release gcc wget
}

function is_centos {
    [[ -f /etc/centos-release ]]
}

function is_deb {
    [[ -f /etc/debian_version ]]
}

function install {
    if is_deb; then
        install_deb
    elif is_centos; then
        install_centos
    else
        echo "Unknown distribution"
        exit 1
    fi

    wget https://bootstrap.pypa.io/ez_setup.py -O - | sudo python -
    sudo easy_install pip
    sudo pip install tox
}

install
