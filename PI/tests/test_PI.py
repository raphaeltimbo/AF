import pytest
import PI


def test_get_tag():
    with pytest.raises(ValueError):
        PI.get_tag('FV-290.009')
    PI.config.current_server = PI.get_server('pi-rnce')
    assert PI.config.current_server.Name == 'pi-rnce'
    tag = PI.get_tag('FV-290.009')

