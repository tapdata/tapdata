require 'minitest/autorun'
require 'open3'

class BuildToolsTest < Minitest::Test
  def bootstrap(failure = '')
    source = File.read(File.expand_path('../../build/build.sh', __dir__))
    block = source.split('  binfmt_image=', 2).fetch(1).split('  info ">> registry cache:', 2).first
    script = <<~BASH
      info() { :; }
      sleep() { :; }
      timeout() { shift 2; "$@"; }
      docker() {
        echo "$*"
        case "$1" in
          pull) [[ "$FAILURE" != pull ]];;
          run) [[ "$FAILURE" != run ]];;
          buildx) [[ "$2" != "$FAILURE" ]];;
        esac
      }
      builder_name=test-builder
      bootstrap() {
        binfmt_image=#{block}
      }
      bootstrap
    BASH
    Open3.capture3({'FAILURE' => failure}, 'bash', '-c', script)
  end

  def test_success_uses_pinned_harbor_images
    out, err, status = bootstrap
    assert status.success?, err
    assert_equal 2, out.lines.count { |line| line.start_with?('pull ') }
    assert_match(/binfmt@sha256:[a-f0-9]{64}/, out)
    assert_match(/--driver-opt image=harbor.internal.tapdata.io\/tapdata\/buildkit@sha256:[a-f0-9]{64}/, out)
    assert_includes out, 'run --pull=never'
    refute_includes out, 'docker.io'
  end

  def test_failures_stop_bootstrap_and_pull_is_retried
    %w[pull run create inspect].each do |failure|
      out, err, status = bootstrap(failure)
      refute status.success?, "#{failure}: #{err}"
      assert_equal 3, out.lines.count { |line| line.start_with?('pull ') } if failure == 'pull'
      refute_includes out, 'buildx create' if %w[pull run].include?(failure)
      refute_includes out, 'buildx inspect' if failure == 'create'
    end
  end
end
