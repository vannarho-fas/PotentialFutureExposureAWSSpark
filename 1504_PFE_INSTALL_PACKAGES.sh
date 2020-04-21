# Bootstrap script installing Boost, QuantLib
# and other python packages
# Use to create an EC2 AMI for an EMR Cluster

#!/bin/bash -xe


# ----------------------------------------------------------------------
#              Install Tools        
# ----------------------------------------------------------------------

yum groupinstall -y "Development Tools"
amazon-linux-extras install -y epel
yum install -y python3-pip

# ----------------------------------------------------------------------
#              Install Boost  
# ----------------------------------------------------------------------

echo "Install Boost"
pip3 install boost

# ----------------------------------------------------------------------
#              Install QuantLib And Dependenciesls
# ----------------------------------------------------------------------

# Install QuantLib dependencies

echo "Install QuantLib dependencies"
pip3 install graphviz 
pip3 install emacs 
pip3 install PyLaTeX
pip3 install latexpages

# install

echo "Install QuantLib dependencies"
pip3 install QuantLib-Python

# ----------------------------------------------------------------------
#                    Install Additional Packages              
# ----------------------------------------------------------------------

echo " Install Additional Packages"
pip3 install pypandoc
pip3 install pyspark
pip3 install pandas
pip3 install matplotlib
pip3 install boto3
pip3 install cairocffi

# ----------------------------------------------------------------------
#                        Environment vars          
# ----------------------------------------------------------------------

echo "Set Environment vars"
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
export PYSPARK_PYTHON=/usr/bin/python3
export PYTHONPATH=/usr/bin/python3

ldconfig

# ----------------------------------------------------------------------
#                         Security Update            
# ----------------------------------------------------------------------
echo " Security Update"
yum -y update
