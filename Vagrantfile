# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.require_version ">= 1.6.0"
VAGRANTFILE_API_VERSION = "2"

# This is nice to have
CURDIR = File.dirname(__FILE__)
TESTDIR = CURDIR + '/tests/'
PLAYBOOK = 'site.yml'

require 'yaml'

vagenv = YAML.load_file(File.expand_path('base_env.yaml', TESTDIR))


# {{{ Helper functions

def sync_dirs(vm, host)
  if host.has_key?('synced_dirs')
    dirs = host['synced_dirs']

    dirs.each do |dir|
      vm.synced_folder dir['local'], dir['remote'], dir['options']
    end
  end
end


def provision_ansible(config)
  # Provisioning configuration for Ansible (for Mac/Linux hosts).
  config.vm.provision "ansible" do |ansible|
    ansible.playbook = PLAYBOOK
    ansible.verbose = 'v'
    ansible.sudo = true
  end
end
# }}}

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

    # Is vagrant-cachier present?
    if Vagrant.has_plugin?('vagrant-cachier')
        config.cache.scope = :box
        config.cache.auto_detect = true
    else
        puts "[-] WARN: Save time with 'vagrant plugin install vagrant-cachier'"
    end

    vagenv.each do |vagenv|
        
        if not vagenv['enabled']
          next
        end
        
        config.vm.define vagenv["name"] do |srv|

            srv.vm.box = vagenv["box"]
	        srv.vm.network "public_network"   ## use public_network
            #srv.vm.network "private_network", ip: vagenv["ip"]  ## or private network
            srv.vm.hostname = vagenv["hostname"]

            srv.ssh.pty = true
            srv.ssh.shell = "bash -c 'BASH_ENV=/etc/profile exec bash'"
            srv.ssh.forward_agent = true

            sync_dirs(srv.vm, vagenv)

            # modify vm - allocate resources
            vm_args = [ "modifyvm", :id ]
            vagenv['resource'].collect { |k, v| vm_args |= ["--#{k}", v.to_s] }
            srv.vm.provider "virtualbox" do |vb|
		        vb.customize vm_args
	        end

            # provision ansible
            provision_ansible(config)
    
            ## execute a script
            srv.vm.provision "shell", path: vagenv["bootstrap"]
        end
    end
end
