// Code generated by "stringer"; DO NOT EDIT.

package tenantcapabilities

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[CanAdminRelocateRange-1]
	_ = x[CanAdminScatter-2]
	_ = x[CanAdminSplit-3]
	_ = x[CanAdminUnsplit-4]
	_ = x[CanViewNodeInfo-5]
	_ = x[CanViewTSDBMetrics-6]
	_ = x[ExemptFromRateLimiting-7]
	_ = x[TenantSpanConfigBounds-8]
	_ = x[MaxCapabilityID-8]
}

const _CapabilityID_name = "can_admin_relocate_rangecan_admin_scattercan_admin_splitcan_admin_unsplitcan_view_node_infocan_view_tsdb_metricsexempt_from_rate_limitingspan_config_bounds"

var _CapabilityID_index = [...]uint8{0, 24, 41, 56, 73, 91, 112, 137, 155}

func (i CapabilityID) String() string {
	i -= 1
	if i >= CapabilityID(len(_CapabilityID_index)-1) {
		return "CapabilityID(" + strconv.FormatInt(int64(i+1), 10) + ")"
	}
	return _CapabilityID_name[_CapabilityID_index[i]:_CapabilityID_index[i+1]]
}
