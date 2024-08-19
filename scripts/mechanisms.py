CPU_OVERHEAD = 400

def get_mechanism_conf(config, num_apps, mechanism):

    if mechanism is None: return ""

    conf = []
    memory = 0
    memory_avail = config['spark']['memory']
    cpu_avail = config['kubernetes']['cpu'] * 1000
    num_nodes_avail = config['kubernetes']['nodes']
    num_worker_nodes = num_nodes_avail - num_apps

    if num_worker_nodes <= 0:
        raise ValueError(f"Not enough nodes available for workers. {num_nodes_avail} nodes available, of which {num_apps} are needed for drivers.")

    match mechanism:

        case 'static':
            memory = memory_avail
            executors = num_worker_nodes // num_apps

            if executors <= 0:
                raise ValueError(f"Too many apps for the available worker nodes. {num_worker_nodes} worker nodes available, but at least {num_apps} needed.")
            
            conf.append(f"spark.executor.instances={executors}")
            pass

        case 'dynamic':
            memory = memory_avail
            conf.append("spark.dynamicAllocation.enabled=true")
            conf.append("spark.dyanmicAllocation.minExecutors=0")
            conf.append(f"spark.dynamicAllocation.maxExecutors={num_worker_nodes}")
            conf.append("spark.dynamicAllocation.initialExecutors=0")
            conf.append(f"spark.kubernetes.allocation.batch.size={num_worker_nodes // num_apps}")
            conf.append(f"spark.dynamicAllocation.executorIdleTimeout=15s")
            conf.append(f"spark.dynamicAllocation.shuffleTracking.enabled=true")
            conf.append(f"spark.dynamicAllocation.shuffleTracking.timeout=15s")
            pass

        case 'shared':
            memory = memory_avail // num_apps
            # currently rounding down the memory to full GBs, could be changed to MB instead to allow better distribution...
            
            cpu = (cpu_avail - CPU_OVERHEAD) // num_apps 
            if cpu <= 0:
                raise ValueError(f"Not enough cpu resources available. Computed share per executor: {cpu}m")
            conf.append(f"spark.kubernetes.executor.request.cores={cpu}m")

            executors = num_worker_nodes
            conf.append(f"spark.executor.instances={executors}")

            pass

        case _:
            raise ValueError(f"Unknown mechanism name: '{mechanism}'.")
    
    conf.append(f"spark.executor.memory={memory}g")

    return ' '.join(list(map(lambda s: "--conf " + s, conf)))

