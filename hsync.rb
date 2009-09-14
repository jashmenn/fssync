require 'java'
require 'hadoop-0.20.0-core.jar'
require 'pp'
require 'find'


module HSync #:nodoc:
  module CoreExtensions #:nodoc:
    module Hash #:nodoc:
      module Diff
        def self.do_deep_diff(a, b)
          ma = do_deep_diff2(a, b)
          # bs = do_deep_diff2(b, a)
          # as.merge(bs)
          mb = []

          [ma, mb]
        end

        def self.do_deep_diff2(a, b)
          results = {}
          b.each do |k,v|
            if a.has_key?(k)
              if v.kind_of?(Hash)
                results[k] = do_deep_diff2(v, a[k])
              else
                # results[k] = v
              end
            else
              results[k] = v
            end
          end
          results
        end


        # Returns a hash that represents the difference between two hashes.
        #
        # Examples:
        #
        #   {1 => 2}.diff(1 => 2)         # => {}
        #   {1 => 2}.diff(1 => 3)         # => {1 => 2}
        #   {}.diff(1 => 2)               # => {1 => 2}
        #   {1 => 2, 3 => 4}.diff(1 => 2) # => {3 => 4}
        def deep_diff(h2)
          HSync::CoreExtensions::Hash::Diff.do_deep_diff(self, h2)
        end

        private
      end
    end
  end
end

class Hash
  include HSync::CoreExtensions::Hash::Diff
end

module HSync
  def self.compare(a, b)
    r = Results.new
    ma,mb = *a.deep_diff(b)
    r.files_missing_in_a = ma
    r.files_missing_in_b = mb
    r
  end

class Results < Struct.new(:files_missing_in_a, :files_missing_in_b, :files_newer_in_a, :files_newer_in_b)
end

# gather files -> build directory tree A
#                 build directory tree B 
# 
# calculate the 6 diffs ->
# 
#     A:Source   |  B:Dest     |  Type     |  Action       
#     exists     | not exist   | File      | copy A -> B   
#     exists     | not exist   | Directory | mkdir B       
#     not exist  | exists      | File      | no action     
#     not exist  | exists      | Directory | no action     
#     newer      | older       | File      | copy A -> B   
#     older      | newer       | File      | warning       
#
# perform the action on 

class Node
  include Comparable

  attr_accessor :replication, :length, :owner, :group, :path, :kind, :mtime
  def initialize(replication=nil, length=nil, owner=nil, group=nil, path=nil, kind=nil, mtime=nil)
    @replication, @length, @owner, @group, @path, @kind, @mtime = replication, length, owner, group, path, kind, mtime
  end

  def is_dir?
    kind == "dir" ? true : false
  end

  def eql?(other)
    puts "called eql"
    %w{group length mtime owner path replication kind}.each do |at| 
      return false unless self.send(at) == other.send(at)
    end
    true
  end

  def ==(other)
    eql?(other)
  end
end

class FsShellProxy
  attr_accessor :shell

  def initialize
    @shell = org.apache.hadoop.fs.FsShell.new
    conf = org.apache.hadoop.conf.Configuration.new
    @shell.setConf(conf)
  end
  def conf; @shell.getConf; end

  def ls(srcf, recurse=false)
    files ||= {}
    srcPath = org.apache.hadoop.fs.Path.new(srcf);
    srcFs = srcPath.getFileSystem(conf);
    srcs = srcFs.globStatus(srcPath);
    raise "Connot access #{srcf}: No such file or directory." if !srcs || srcs.length == 0 

    srcs.each do |src|
      items = shell_list_status(srcFs, src)
      items.each do |stat|
        cur = stat.getPath()
        path = cur.toUri().getPath();

        if stat.isDir && recurse
          files[File.basename(path)] = ls(path, recurse)
        else
          files[File.basename(path)] = Node.new(replication=stat.getReplication, length=stat.getLen, 
                              owner=stat.getOwner, group=stat.getGroup, 
                              path=path, kind=(stat.isDir ? "dir" : "file"),
                              mtime=stat.getModificationTime);
        end
      end
    end

    files
  end

  def shell_list_status(srcFs, src)
    return [src] if !src.isDir
    path = src.getPath
    srcFs.listStatus(path)
  end

end


class LocalFs
  def ls(srcf, recurse=false)
    f ||= {}
    raise "Connot access #{srcf}: No such file or directory." unless File.exists?(srcf)

    files = Dir["#{srcf}/*"]
      files.each do |path|
        if File.directory?(path)
          f[File.basename(path)] = ls(path, recurse)
        else
          stat = File.stat(path)
          f[File.basename(path)] = Node.new(
                              replication=nil, length=stat.size, 
                              owner=stat.uid, group=stat.gid, 
                              path=path, kind=(stat.directory? ? "dir" : "file"),
                              mtime=(File.mtime(path).to_f * 1000).to_i)
        end
      end
    f
  end
end

end


if $0 == __FILE__

include HSync

path = ARGV[0] || "/"
local_nodes = LocalFs.new.ls(path, true)
pp local_nodes

# shell = FsShellProxy.new
# nodes = shell.ls(path, true)
# pp nodes

# p shell.run(["-ls", "/"].to_java(:string))
# nodes.each do |n|
  # puts "%-60s %20s %s" % [n.path, n.length, n.mtime]
# end

end 
