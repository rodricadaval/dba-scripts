# creating dynamic inventory

# RUN 

cd git_root/ansible

python3 make_global_inventory.py

# All inventory hosts are gonna be added in group_vars folder separated by country or profile.
# Each profile/country divided by engine (postgres, mysql, aurora, docdb, etc) 

# To check hosts related to a country/profile and service run like this:

ansible-inventory -i group_vars/ar/dynamic_inventory.yaml --graph docdb

# To run a connectivity check playbook here is an example:

ansible-playbook -i group_vars/xx-xx-west-2/dynamic_inventory.yaml --extra-var "service=docdb"  playbooks/check_with_shell.yml




