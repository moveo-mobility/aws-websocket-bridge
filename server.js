async function resolveDeviceAndVehicleIds(serialNumber) {
  if (!serialNumber) return { device_id: null, vehicle_id: null };

  try {
    const { data: deviceData, error: deviceError } = await supabase
      .from('telematic_devices')
      .select('device_id')
      .eq('device_serial', serialNumber)
      .single();

    if (deviceError || !deviceData) {
      console.log('Device not found for serial number:', serialNumber);
      return { device_id: null, vehicle_id: null };
    }

    const device_id = deviceData.device_id;

    const { data: assignmentData, error: assignmentError } = await supabase
      .from('vehicle_telematic_assignments')
      .select('vehicle_id')
      .eq('device_id', device_id)
      .is('unassigned_at', null)  // ← ADD THIS LINE - filters for only active assignments
      .single();

    if (assignmentError || !assignmentData) {
      console.log('Vehicle assignment not found for device_id:', device_id);
      return { device_id, vehicle_id: null };
    }

    return { 
      device_id: device_id, 
      vehicle_id: assignmentData.vehicle_id 
    };

  } catch (error) {
    console.error('Error resolving device/vehicle IDs:', error);
    return { device_id: null, vehicle_id: null };
  }
}
