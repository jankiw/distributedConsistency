from eclypse.placement.strategies import PlacementStrategy


class MyStrategy(PlacementStrategy):
    def place(self, infrastructure, application, placements, placement_view):
        #services are named the same as nodes
        return {
            node: node
            for node in application.nodes
        }
