require 'digest'
require 'fileutils'
require 'minitest/autorun'
require 'open3'
require 'tmpdir'

class FrontendToolingTest < Minitest::Test
  ROOT = File.expand_path('../..', __dir__)

  def executable(path, script)
    File.write(path, "#!/bin/bash\nset -eu\n#{script}\n")
    File.chmod(0o755, path)
  end

  def test_build_uses_the_verified_node_and_project_pnpm
    Dir.mktmpdir('tapdata-frontend') do |workspace|
      repo = File.join(workspace, 'tapdata')
      frontend = File.join(workspace, 'tapdata-enterprise-web')
      runner_bin = File.join(workspace, 'runner-bin')
      github_path = File.join(workspace, 'github-path')
      FileUtils.mkdir_p([repo, runner_bin, File.join(frontend, 'build')])
      FileUtils.cp_r(File.join(ROOT, 'build'), repo)
      File.write(github_path, '')

      version = File.read(File.join(repo, 'build/frontend-node-version')).strip
      assert_match(/^22\./, version)
      release = "node-v#{version}-linux-x64"
      release_bin = File.join(workspace, release, 'bin')
      FileUtils.mkdir_p(release_bin)
      executable(File.join(release_bin, 'node'), "echo v#{version}")
      executable(File.join(release_bin, 'corepack'), <<~SH)
        [[ "$1 $2 $4" == 'enable --install-directory pnpm' ]]
        ln -s "$FIXTURE_DIR/project-pnpm" "$3/pnpm"
      SH
      archive = "#{release}.tar.xz"
      assert system('tar', '-cJf', archive, release, chdir: workspace)
      checksum = Digest::SHA256.file(File.join(workspace, archive)).hexdigest
      File.write(File.join(workspace, 'SHASUMS256.txt'), "#{checksum}  #{archive}\n")

      executable(File.join(runner_bin, 'node'), 'echo v20.20.2')
      executable(File.join(runner_bin, 'nvm'), 'echo "Unexpected Node version switch" >&2; exit 42')
      executable(File.join(runner_bin, 'uname'), 'echo x86_64')
      executable(File.join(runner_bin, 'pnpm'), 'echo "Unexpected runner pnpm" >&2; exit 43')
      executable(File.join(workspace, 'project-pnpm'), 'echo 10.32.1')
      executable(File.join(runner_bin, 'curl'), <<~SH)
        url="${@: -3:1}"
        [[ "$url" == "https://nodejs.org/dist/v#{version}/"* ]]
        cp "$FIXTURE_DIR/${url##*/}" "${@: -1}"
      SH
      executable(File.join(frontend, 'build/build.sh'), <<~SH)
        [[ "$*" == '-m community' && "$DAAS_BUILD_NUMBER" == 'test-build' ]] || exit 1
        node_version=$(node --version) || exit $?
        pnpm_version=$(pnpm --version) || exit $?
        [[ "$node_version" == 'v#{version}' && "$pnpm_version" == '10.32.1' ]] || exit 1
        echo 'Frontend build reached'
      SH

      env = {
        'PATH' => "#{runner_bin}:#{ENV.fetch('PATH')}",
        'RUNNER_TEMP' => workspace,
        'TMPDIR' => workspace,
        'GITHUB_PATH' => github_path,
        'FIXTURE_DIR' => workspace
      }
      output, error, status = Open3.capture3(env, 'bash', 'build/ensure-frontend-node.sh', chdir: repo)
      assert status.success?, "#{output}#{error}"
      assert_includes output, "#{archive}: OK"
      node_bin = File.join(workspace, "frontend-node-#{version}", release, 'bin')
      assert_equal [node_bin], File.readlines(github_path, chomp: true)

      env['PATH'] = "#{node_bin}:#{env.fetch('PATH')}"
      output, error, status = Open3.capture3(env, 'bash', 'build/build.sh', '-c', 'frontend', '-t', 'test-build', chdir: repo)
      assert status.success?, "#{output}#{error}"
      assert_includes output, 'Frontend build reached'
    end
  end
end
