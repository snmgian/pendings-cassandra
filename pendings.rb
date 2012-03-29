require 'cassandra'
require 'sinatra/base'
require 'uuid'

class Pendings < Sinatra::Base

  before do
    @cassandra = Cassandra.new('pendings', '127.0.0.1:9160')
    @uuid = UUID.new
  end

  get '/' do
    @pending_tasks = pending_tasks
    @rejected_tasks = rejected_tasks
    @completed_tasks = completed_tasks
    @count_pending_tasks = count_pending_tasks

    erb :index
  end

  get '/projects' do
    @projects = projects
    erb :'projects/index'
  end

  post '/projects' do
    create_project(params)
    redirect '/projects'
  end

  get '/projects/new' do
    erb :'projects/new'
  end

  post '/tasks' do
    create_task(params)
    redirect '/'
  end

  post '/tasks/:id/complete' do
    complete_task(params[:id])
    redirect '/'
  end

  post '/tasks/:id/reject' do
    reject_task(params[:id])
    redirect '/'
  end

  get '/tasks/new' do
    @projects = projects
    @tags = tags

    erb :'tasks/new'
  end

  private
  def complete_task(key)
    @cassandra.batch do
      @cassandra.remove(:Pending_Tasks, key)
      @cassandra.insert(:Tasks, key, 'status' => 'completed')
    end
  end

  def completed_tasks
    filter_tasks_by_status('completed')
  end

  def count_pending_tasks
    @cassandra.count_range(:Pending_Tasks)
  end

  def create_project(params)
    @cassandra.insert(:Projects, generate_key, 'name' => params[:name])
  end

  def create_task(params)
    @cassandra.batch do
      task_key = generate_key

      @cassandra.insert(:Tasks, task_key, 
                        'title' => params[:title], 
                        'project_id' => params[:project_id],
                        'status' => 'pending')

      tasks_tags = {}
      @params[:tags].each do |tag_key|
        tasks_tags[tag_key] = tag_key
      end

      @cassandra.insert(:Tasks_Tags, task_key, tasks_tags)

      @cassandra.insert(:Pending_Tasks, task_key, 'task_key' => task_key)
    end
  end

  def generate_key
    Time.now.to_i.to_s
  end

  def generate_uuid_key
    @uuid.generate
  end

  def filter_tasks_by_status(status)
    tasks = @cassandra.get_indexed_slices(:Tasks, [{:column_name => 'status', :value => status, :comparison => '=='}])

    tasks = @cassandra.multi_get(:Tasks, tasks.keys)
    projects_keys = tasks.map{|t| t[1]['project_id'].to_s}
    projects = @cassandra.multi_get(:Projects, projects_keys)

    tags = @cassandra.multi_get(:Tasks_Tags, tasks.keys)

    tasks.each do |key, task|
      task['project'] = projects[task['project_id']]
      task['tags'] = tags[key].values || []
    end

    tasks
  end

  def pending_tasks
    filter_tasks_by_status('pending')
  end

  def projects
    @cassandra.get_range(:Projects)
  end

  def reject_task(key)
    @cassandra.batch do
      @cassandra.remove(:Pending_Tasks, key)
      @cassandra.insert(:Tasks, key, 'status' => 'rejected')
    end
  end

  def rejected_tasks
    filter_tasks_by_status('rejected')
  end

  def tags
    @cassandra.get_range(:Tags)
  end
end

