from copy import deepcopy
import pytest
from certify_values import assess_image,compare_derived
from resolve_core import MARKER

COLUMNS=[{'name':'OrderID','type':'int','ordinal':1},
         {'name':'Comments','type':'nvarchar','max_length':-1,'ordinal':2}]
CAPTURED={'row_data':{'OrderID':'7','Comments':None},'update_mask':'0x01'}
ARCHIVED={'OrderID':7,'Comments':MARKER}


def test_source_proof_requires_clear_mask_bit_and_full_image_agreement():
    values,names=assess_image(CAPTURED,ARCHIVED,COLUMNS,'Sales.Orders',3)
    assert values['Comments'] is None and names=={'Comments'}


@pytest.mark.parametrize('mask',['0x03',None,'0x','invalid'])
def test_modified_or_unknown_column_cannot_be_certified(mask):
    with pytest.raises(ValueError):
        assess_image({**CAPTURED,'update_mask':mask},ARCHIVED,COLUMNS,'Sales.Orders',4)


def test_literal_marker_assignment_does_not_become_a_preserved_old_value():
    captured=deepcopy(CAPTURED);captured['row_data']['Comments']=MARKER
    with pytest.raises(ValueError): assess_image(captured,ARCHIVED,COLUMNS,'Sales.Orders',4)


def test_unrelated_corruption_cannot_hide_behind_a_marker():
    with pytest.raises(ValueError): assess_image(CAPTURED,{**ARCHIVED,'OrderID':8},COLUMNS,'Sales.Orders',4)


def test_missing_column_is_not_null_and_unknown_table_is_rejected():
    with pytest.raises(ValueError): assess_image(CAPTURED,{'OrderID':7},COLUMNS,'Sales.Orders',3)
    with pytest.raises(ValueError): assess_image(CAPTURED,ARCHIVED,COLUMNS,'Sales.Unknown',3)


def test_unavailable_source_null_does_not_overwrite_a_proven_nonnull_value():
    assert not compare_derived({'Comments':None,'OrderID':'7'}, {'Comments':'known','OrderID':'7'},
                               {'Comments'},{'Comments':'known'})


def test_true_null_and_exact_capture_are_reported_separately():
    assert compare_derived({'Comments':None},{'Comments':None},{'Comments'},{'Comments':None})


def test_derived_value_must_match_origin_and_every_other_source_field():
    for result in [{'Comments':None,'OrderID':'7'},{'Comments':'known','OrderID':'8'}]:
        with pytest.raises(ValueError):
            compare_derived({'Comments':None,'OrderID':'7'},result,{'Comments'},{'Comments':'known'})
