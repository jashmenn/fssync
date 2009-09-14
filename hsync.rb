require 'java'
require 'hadoop-0.20.0-core.jar'
require 'pp'
require 'find'

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
  attr_accessor :replication, :length, :owner, :group, :path, :type, :mtime
  def initialize(replication=nil, length=nil, owner=nil, group=nil, path=nil, type=nil, mtime=nil)
    @replication, @length, @owner, @group, @path, @type, @mtime = replication, length, owner, group, path, type, mtime
  end

  def is_dir?
    type == "dir" ? true : false
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
                              path=path, type=(stat.isDir ? "dir" : "file"),
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
                              path=path, type=(stat.directory? ? "dir" : "file"),
                              mtime=(File.mtime(path).to_f * 1000).to_i)
        end
      end
    f
  end
end



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

  
