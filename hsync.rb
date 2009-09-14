require 'java'
require 'hadoop-0.20.0-core.jar'
require 'pp'
require 'logger'

module HSync #:nodoc:
  module CoreExtensions #:nodoc:
    module Hash #:nodoc:
      module Diff
        def self.do_deep_diff(a, b, opts={}, block=nil)
          ma = do_deep_diff2(a, b, opts, block)
          mb = do_deep_diff2(b, a, opts, block)
          [ma, mb]
        end

        def self.do_deep_diff2(a, b, opts={}, block=nil)
          results = {}
          b.each do |k,v|
            if a.has_key?(k)
              if v.kind_of?(::Hash)
                returned = do_deep_diff2(v, a[k], opts, block)
                if returned && returned.size > 0
                  results[k] = returned
                end
              else
                if m = opts[:intersection]
                  # if the return value of m isnt equal, call the conflict
                  # resolution block
                  unless v.send(m) == a[k].send(m)
                    if returned = block.call(v, a[k])
                      results[k] = returned
                    end
                  end
                end
              end
            else
              unless opts[:intersection]
                results[k] = v
              end
            end
          end
          results
        end

        def deep_diff(h2, opts={}, &block)
          HSync::CoreExtensions::Hash::Diff.do_deep_diff(self, h2, opts, block)
        end

        def to_paths
          do_to_paths(self)
        end

        private
        def do_to_paths(input, prefix=[], paths=[])
          if input.kind_of?(::Hash)
            input.each do |k,v|
              if (result = do_to_paths(v, prefix.dup << k, paths)) && result.kind_of?(String)
                paths << result
              end
            end
          else
            return prefix.join("/")
          end
          paths
        end

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

    na,nb = *a.deep_diff(b, {:intersection => :mtime}) do |node1, node2| 
      node1.mtime > node2.mtime ? nil : node2
    end
    r.files_newer_in_a = na
    r.files_newer_in_b = nb

    r
  end

  def self.sync(source, dest)
    Synchro.new(source, dest).sync!
  end

class Synchro
  def initialize(source, dest)
    @source, @dest = source, dest
  end

  #     A:Source   |  B:Dest     |  Type     |  Action       
  #     exists     | not exist   | File      | copy A -> B   
  #     exists     | not exist   | Directory | mkdir B       
  #     not exist  | exists      | File      | no action     
  #     not exist  | exists      | Directory | no action     
  #     newer      | older       | File      | copy A -> B   
  #     older      | newer       | File      | warning       
  def sync!

    destfiles = begin 
                  FsShellProxy.new.ls(@dest, true) 
                rescue NoSuchFile 
                  {}
                end
    results = HSync::compare(LocalFs.new.ls(@source, true), destfiles)
    push_files(results.files_missing_in_b)
    # push_files(results.files_newer_in_a)
  end

  def push_files(files)
    shell = FsShellProxy.new
    f = files.to_paths
    logger.info("pushing #{f.size} files")
    logger.info(f.pretty_inspect)

    f.each do |file|
      src = "#{@source}/#{file}"
      dest = "#{@dest}/#{file}" 
      logger.info("cp #{src} #{dest}")
      shell.cp(src, dest)
    end
  end

  def logger
    @logger ||= begin 
              logger = Logger.new(@logdev || STDOUT)
              logger.formatter = Logger::Formatter.new
              logger.datetime_format = "%Y-%m-%d %H:%M:%S"
              logger
            end
  end
end

class Results < Struct.new(:files_missing_in_a, :files_missing_in_b, :files_newer_in_a, :files_newer_in_b)
end

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
    %w{group length mtime owner path replication kind}.each do |at| 
      return false unless self.send(at) == other.send(at)
    end
    true
  end

  def ==(other)
    eql?(other)
  end
end

class NoSuchFile < StandardError; end
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
    raise(NoSuchFile, "Connot access #{srcf}: No such file or directory.") if !srcs || srcs.length == 0 

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

  def cp(srcf, destf) 
    dstPath = org.apache.hadoop.fs.Path.new(destf)
    srcs = org.apache.hadoop.fs.Path.new(srcf)
    dstFs = dstPath.getFileSystem(conf)
    dstFs.copyFromLocalFile(false, false, srcs, dstPath)
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
raise "usage: localpath hdfspath" unless ARGV.size >= 2

HSync.sync(ARGV[0], ARGV[1])

# path = ARGV[0] || "/"
# local_nodes = LocalFs.new.ls(path, true)
# pp local_nodes

# shell = FsShellProxy.new
# nodes = shell.ls(path, true)
# pp nodes

# p shell.run(["-ls", "/"].to_java(:string))
# nodes.each do |n|
  # puts "%-60s %20s %s" % [n.path, n.length, n.mtime]
# end

end 
