package sarama

type ListGroupsRequest struct {
	Version      int16
	StatesFilter []string // version 4 or later
}

func (r *ListGroupsRequest) encode(pe packetEncoder) error {
	if r.Version >= 4 {
		pe.putCompactArrayLength(len(r.StatesFilter))
		for _, filter := range r.StatesFilter {
			err := pe.putCompactString(filter)
			if err != nil {
				return err
			}
		}
	}
	if r.Version >= 3 {
		pe.putEmptyTaggedFieldArray()
	}
	return nil
}

func (r *ListGroupsRequest) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version
	if r.Version >= 4 {
		filterLen, err := pd.getCompactArrayLength()
		if err != nil {
			return err
		}
		if filterLen > 0 {
			r.StatesFilter = make([]string, filterLen)
			for i := 0; i < filterLen; i++ {
				if r.StatesFilter[i], err = pd.getCompactString(); err != nil {
					return err
				}
			}
		}
	}
	if r.Version >= 3 {
		if _, err = pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}
	return nil
}

func (r *ListGroupsRequest) key() int16 {
	return 16
}

func (r *ListGroupsRequest) version() int16 {
	return r.Version
}

func (r *ListGroupsRequest) headerVersion() int16 {
	if r.Version >= 3 {
		return 2
	}
	return 1
}

func (r *ListGroupsRequest) isValidVersion() bool {
	return r.Version >= 0 && r.Version <= 4
}

func (r *ListGroupsRequest) requiredVersion() KafkaVersion {
	switch r.Version {
	case 4:
		return V2_6_0_0
	case 3:
		return V2_4_0_0
	case 2:
		return V2_0_0_0
	case 1:
		return V0_11_0_0
	case 0:
		return V0_9_0_0
	default:
		return V2_6_0_0
	}
}

func (r *ListGroupsRequest) SetVersion(v KafkaVersion) {
	switch {
	case v == Automatic:
	case v.IsAtLeast(V2_6_0_0):
		// Version 4 adds the StatesFilter field (KIP-518).
		r.Version = 4
	case v.IsAtLeast(V2_4_0_0):
		// Version 3 is the first flexible version.
		r.Version = 3
	case v.IsAtLeast(V2_0_0_0):
		// Version 2 is the same as version 0.
		r.Version = 2
	case v.IsAtLeast(V0_11_0_0):
		// Version 1 is the same as version 0.
		r.Version = 1
	default:
		r.Version = 0
	}
}

func (r *ListGroupsRequest) supportedVersions() (int16, int16) {
	return 0, 4
}

func (r *ListGroupsRequest) setVersion(v int16) {
	r.Version = v
}
