package main

func firstInstance(instances []string) string {
	if len(instances) == 0 {
		return ""
	}
	return instances[0]
}
