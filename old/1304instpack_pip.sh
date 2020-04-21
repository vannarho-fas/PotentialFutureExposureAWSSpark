# Bootstrap script installing Anaconda Python, compiling Boost, then QuantLib
# then QuantLib SWIG wrappers 
# Use to create an EC2 AMI for an EMR Cluster

#!/bin/bash -xe

# install packages in /usr/local if you have admin rights and want everyone to use the software

sudo cd /usr/local
INSTDR=/usr/local

# ----------------------------------------------------------------------
#              Install Tools        
# ----------------------------------------------------------------------


sudo amazon-linux-extras install epel
sudo yum install python3-pip


# ----------------------------------------------------------------------
#              Install Boost  
# ----------------------------------------------------------------------

echo "Install Boost"

sudo pip3 install boost

# ----------------------------------------------------------------------
#              Install QuantLib
# ----------------------------------------------------------------------

# Install QuantLib dependencies

echo "Install QuantLib dependencies"
sudo pip3 install graphviz emacs PyLaTeX boost-devel latexpages

# install

echo "Install QuantLib dependencies"
sudo pip3 install QuantLib


# ----------------------------------------------------------------------
#                    Install Additional Packages              
# ----------------------------------------------------------------------

echo " Install Additional Packages"
sudo pip3 install pandas matplotlib boto3 cairocffi

# ----------------------------------------------------------------------
#                        Environment vars          
# ----------------------------------------------------------------------

echo "Set Environment vars"
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
export PYSPARK_PYTHON=/usr/bin/python3
export PYTHONPATH=/usr/bin/python3

sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 1

sudo ldconfig

# ----------------------------------------------------------------------
#                         Security Update            
# ----------------------------------------------------------------------
echo " Security Update"
sudo yum -y update
sudo pip3 update
