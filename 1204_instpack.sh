# Bootstrap script installing Anaconda Python, compiling Boost, then QuantLib
# then QuantLib SWIG wrappers 
# Use to create an EC2 AMI for an EMR Cluster

#!/bin/bash -xe

# install packages in /usr/local if you have admin rights and want everyone to use the software

sudo cd /usr/local
INSTDR=/usr/local

# ----------------------------------------------------------------------
#              Install Dev Tools to Compile Packages         
# ----------------------------------------------------------------------

echo "Install Dev Tools"
sudo yum groupinstall -y "Development Tools"

# ----------------------------------------------------------------------
#              Install Anaconda             
# ----------------------------------------------------------------------
# https://docs.anaconda.com/anaconda/install/linux/

echo "Install Dependencies"
sudo yum install -y libXcomposite libXcursor libXi libXtst libXrandr alsa-lib mesa-libEGL libXdamage mesa-libGL libXScrnSaver

echo "Download Anaconda"
sudo wget https://repo.anaconda.com/archive/Anaconda3-2020.02-Linux-x86_64.sh

# adjust to latest version as needed
echo "Run Anaconda Shell"
sudo bash ./Anaconda3-2020.02-Linux-x86_64.sh -b -p $INSTDR/anaconda

echo "Delete Anaconda Shell"
sudo rm Anaconda3-2020.02-Linux-x86_64.sh

echo "Set Anaconda Paths"
export PATH=$INSTDR/anaconda/bin:$PATH


# source ~/.bashrc

# test with "conda list"
# https://docs.anaconda.com/anaconda/install/verify-install/


# ----------------------------------------------------------------------
#              Prepare Boost & DLIB    
# ----------------------------------------------------------------------

# There are always three steps to install software on Linux systems:

# configure — "check"
# make — "build software in current directory"
# make install — "copy files to the systems so the other software can use this software"

# Download
# get latest version from https://www.boost.org/

echo "Download & Extract Boost"

sudo wget https://dl.bintray.com/boostorg/release/1.72.0/source/boost_1_72_0.tar.gz

# Extract

sudo tar -xvf boost_1_72_0.tar.gz

# Remove tarballs

sudo rm boost_1_72_0.tar.gz


# ----------------------------------------------------------------------
#              Install Boost  
# ----------------------------------------------------------------------

cd boost_1_72_0

# Prepare

echo "Prepare Boost"
sudo bash ./bootstrap.sh — with-python=python3 — with-libraries=python — prefix=/usr

# Build

echo "Build Boost"
sudo ./b2

# Install

echo "Install Boost"
sudo ./b2 install

# Vars

echo "Set Env Vars Boost"
export BOOST_INCLUDEDIR=$INSTDR/boost_1_72_0
export BOOST_ROOT=$INSTDR/boost_1_72_0
export BOOST_LIBRARYDIR=$INSTDR/boost_1_72_0/stage/lib

cd ../

# ----------------------------------------------------------------------
#              Install QuantLib
# ----------------------------------------------------------------------

# Install QuantLib dependencies

echo "Install QuantLib dependencies"
sudo yum install -y graphviz emacs texlive boost-devel

# download

echo "Download QuantLib"
sudo wget https://pfefiles.s3-ap-southeast-2.amazonaws.com/QuantLib-1.18.tar.gz

# Unpack

echo "Unpack QuantLib"
sudo tar xzf QuantLib-1.18.tar.gz

# Prepare

echo "Configure QuantLib"
cd QuantLib-1.18

# sudo ./configure PYTHON=$INSTDR/anaconda/bin/python3
sudo ./configure --with-boost-include=$INSTDR/include/ --with-boost-lib=$INSTDR/lib/ --disable-shared --enable-static

# Make

echo "Make QuantLib"
sudo make

#Install

echo "Install QuantLib"
sudo make install
sudo ldconfig

# Remove tarball

echo "Clean up QuantLib tarball"
cd ../
sudo rm QuantLib-1.18.tar.gz

# you can test the QL installation at this point
# cd QuantLib-1.18/Examples/BermudanSwaption
#  g++ BermudanSwaption.cpp -o BermudanSwaption -lQuantLib
#  ./BermudanSwaption
g++ CDS.cpp -o CDS1 -lQuantLib
  
# Vars

echo "Set QuantLib Vars"
LD_LIBRARY_PATH=/usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH
export PATH=$INSTDR/QuantLib-1.18:$PATH

# quantlib-config

sudo ldconfig


# ----------------------------------------------------------------------
#              Install QuantLib-SWIG
# ----------------------------------------------------------------------

#Download
# You can download released QuantLib-SWIG versions from Bintray at https://bintray.com/quantlib/releases/QuantLib-SWIG.

echo "Get QuantLib-SWIG"
sudo wget https://pfefiles.s3-ap-southeast-2.amazonaws.com/QuantLib-SWIG-1.18.tar.gz

#Unpack

echo "Unpack QuantLib-SWIG"
sudo tar xzf QuantLib-SWIG-1.18.tar.gz

#Configure

echo "Configure QuantLib-SWIG"
cd QuantLib-SWIG-1.18

sudo ./configure PYTHON=$INSTDR/anaconda/bin/python3

# Make

echo "Make QuantLib-SWIG"

cd Python
sudo /usr/local/anaconda/bin/python3 setup.py install

sudo make -C Python

# Install

echo "Install QuantLib-SWIG"
sudo make -C Python install
# make -C Python check

#Cleam up
echo "Cleanup QuantLib-SWIG"
cd ../
sudo rm QuantLib-SWIG-1.18.tar.gz


# ----------------------------------------------------------------------
#                    Install Additional Packages              
# ----------------------------------------------------------------------

echo " Install Additional Packages"
sudo su conda install -y pandas 
conda install -y matplotlib

# ----------------------------------------------------------------------
#                        Environment vars          
# ----------------------------------------------------------------------

echo "Set Environment vars"
export PYSPARK_DRIVER_PYTHON=$INSTDR/anaconda/bin/python3
export PYSPARK_PYTHON=$INSTDR/anaconda/bin/python3
export PYTHONPATH=$INSTDR/anaconda/bin/python3
sudo ldconfig
# sudo chmod 444 /var/aws/emr/userData.json

# ----------------------------------------------------------------------
#                         Security Update            
# ----------------------------------------------------------------------
echo " Security Update"
sudo yum -y update
